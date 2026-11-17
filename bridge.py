"""Authenticated SoundCloud and YouTube audio adapter."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import hmac
import base64
import ipaddress
import socket
from collections import OrderedDict
from contextlib import nullcontext
import os
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Lock, BoundedSemaphore
import time
from urllib.parse import parse_qs, urlsplit, urlunsplit, urlencode, urljoin
from urllib.request import Request, build_opener, HTTPRedirectHandler
from urllib.error import HTTPError, URLError

PORT = int(os.environ.get('PORT', '8765'))
BIND_HOST = os.environ.get('BIND_HOST', '127.0.0.1')
BRIDGE_TOKEN = os.environ.get('BRIDGE_TOKEN', '')
ALLOWED_HOSTS = set(os.environ.get('ALLOWED_HOSTS', f'127.0.0.1:{PORT},localhost:{PORT}').split(','))
if os.environ.get('RENDER_EXTERNAL_HOSTNAME'):
    ALLOWED_HOSTS.add(os.environ['RENDER_EXTERNAL_HOSTNAME'])
MAX_JOBS = 8
MAX_BYTES = 128 * 1024 * 1024
MAX_CACHE = 128 * 1024 * 1024
TTL = 3600


class RateLimit:
    def __init__(self):
        self.buckets = OrderedDict()
        self.lock = Lock()

    def allow(self, client, route):
        limit = 30 if route in {'/prepare', '/search'} else 600
        key = (client, route)
        now = time.monotonic()
        with self.lock:
            count, started = self.buckets.pop(key, (0, now))
            if now - started >= 60:
                count, started = 0, now
            self.buckets[key] = (count + 1, started)
            while len(self.buckets) > 4096:
                self.buckets.popitem(last=False)
            return count < limit


RATE_LIMIT = RateLimit()


def normalize_url(value):
    parts = urlsplit(value)
    if parts.scheme != 'https' or parts.username or parts.password or parts.port not in {None, 443}:
        raise ValueError('Expected an HTTPS SoundCloud track or YouTube video URL')
    if parts.hostname in {'youtube.com', 'www.youtube.com', 'music.youtube.com', 'm.youtube.com', 'youtu.be'}:
        if parts.hostname == 'youtu.be':
            video_id = parts.path.strip('/')
        elif parts.path == '/watch':
            video_id = parse_qs(parts.query).get('v', [''])[0]
        else:
            match = re.fullmatch(r'/(?:shorts|embed)/([\w-]{11})/?', parts.path)
            video_id = match[1] if match else ''
        if not re.fullmatch(r'[A-Za-z0-9_-]{11}', video_id):
            raise ValueError('Expected a single YouTube video link')
        return 'https://www.youtube.com/watch?v=' + video_id
    segments = parts.path.strip('/').split('/')
    if (parts.scheme != 'https' or parts.hostname not in {'soundcloud.com', 'www.soundcloud.com'}
            or parts.username or parts.password or parts.port not in {None, 443}
            or len(segments) != 2 or not all(re.fullmatch(r'[\w-]+', s) for s in segments)
            or segments[0] in {'discover', 'search', 'charts', 'you', 'settings'}):
        raise ValueError('Expected a public https://soundcloud.com/artist/track URL')
    return urlunsplit(('https', 'soundcloud.com', '/' + '/'.join(segments), '', ''))


SEARCH_GATE = BoundedSemaphore(1)
SEARCH_CACHE = OrderedDict()
SEARCH_LOCK = Lock()


def youtube_search(query, limit=10, offset=0):
    query = str(query).strip()
    if not query or len(query) > 200 or not 1 <= limit <= 20 or not 0 <= offset <= 80:
        raise ValueError('Search requires 1-200 characters, limit 1-20, and offset 0-80')
    key = (query.casefold(), limit, offset)
    with SEARCH_LOCK:
        cached = SEARCH_CACHE.get(key)
        if cached and time.monotonic() - cached[0] < 180:
            SEARCH_CACHE.move_to_end(key)
            return cached[1]
    if not SEARCH_GATE.acquire(blocking=False):
        raise RuntimeError('Search is busy; retry shortly')
    try:
        raw = run_process([
            sys.executable, '-m', 'yt_dlp', '--ignore-config', '--flat-playlist',
            '--dump-single-json', '--skip-download', '--socket-timeout', '10',
            '--retries', '1', '--playlist-start', str(offset + 1),
            '--playlist-end', str(offset + limit), '--', f'ytsearch{offset + limit}:{query}',
        ], 40)
        entries = json.loads(raw).get('entries') or []
        items = []
        for entry in entries:
            video_id = entry.get('id', '')
            if not re.fullmatch(r'[A-Za-z0-9_-]{11}', video_id) or entry.get('is_live'):
                continue
            items.append({'title': entry.get('title') or 'YouTube track',
                          'artist': entry.get('uploader') or entry.get('channel') or 'YouTube',
                          'url': 'https://www.youtube.com/watch?v=' + video_id,
                          'cover': f'https://i.ytimg.com/vi/{video_id}/hqdefault.jpg',
                          'duration': entry.get('duration')})
        result = {'items': items, 'nextOffset': offset + limit, 'done': len(entries) < limit or offset + limit >= 100}
        with SEARCH_LOCK:
            SEARCH_CACHE[key] = (time.monotonic(), result)
            SEARCH_CACHE.move_to_end(key)
            while len(SEARCH_CACHE) > 32:
                SEARCH_CACHE.popitem(last=False)
        return result
    finally:
        SEARCH_GATE.release()


def run_process(args, timeout):
    result = subprocess.run(args, capture_output=True, text=True, encoding='utf-8',
                            errors='replace', timeout=timeout,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    if result.returncode:
        message = result.stderr.strip().splitlines()
        details = [line for line in message if 'WARNING:' in line or 'ERROR:' in line]
        for line in details[-5:]:
            print('Audio extractor: ' + re.sub(r'https?://\S+', '[provider URL]', line)[:400], flush=True)
        raise RuntimeError(message[-1][:300] if message else 'Audio preparation failed')
    return result.stdout


def extract_metadata(url):
    args = [
        sys.executable, '-m', 'yt_dlp', '--ignore-config', '--no-playlist',
        '--skip-download', '--dump-single-json', '--format', 'bestaudio[acodec=mp3]/bestaudio',
        '--socket-timeout', '10', '--retries', '1',
    ]
    configured = os.environ.get('YOUTUBE_PLAYER_CLIENTS', '').strip()
    if configured and urlsplit(url).hostname == 'www.youtube.com':
        if not all(client in {'web', 'mweb', 'tv', 'web_embedded', 'android_vr'} for client in configured.split(',')):
            raise ValueError('Unsupported YouTube player client configuration')
        args += ['--extractor-args', 'youtube:player_client=' + configured]
    try:
        return json.loads(run_process(args + ['--', url], 55))
    except RuntimeError as exc:
        if configured or urlsplit(url).hostname != 'www.youtube.com' or 'Failed to extract any player response' not in str(exc):
            raise
        print('Retrying YouTube metadata with supported alternate clients', flush=True)
        return json.loads(run_process(args + ['--extractor-args', 'youtube:player_client=web_embedded,tv', '--', url], 55))


VIDEOSCALE_BASE = 'https://gate.apiscrape.net:16262'


class NoProviderRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def provider_fields(data):
    return {str(key).lower(): value for key, value in data.items()} if isinstance(data, dict) else {}


def provider_shape(label, data):
    fields = provider_fields(data)
    print('VideoScale response shape: ' + label + ' ' + ','.join(sorted(fields)[:20]), flush=True)


def videoscale_request(path, payload=None, download_location=False):
    username = os.environ.get('VIDEOSCALE_API_USERNAME', '')
    password = os.environ.get('VIDEOSCALE_API_PASSWORD', '')
    if not username or not password:
        raise RuntimeError('VideoScale API credentials are not configured on the audio server')
    token = base64.b64encode((username + ':' + password).encode()).decode()
    headers = {'Authorization': 'Basic ' + token, 'Accept': 'application/json', 'User-Agent': 'WaveMusic/2'}
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        headers['Content-Type'] = 'application/json'
    try:
        response = build_opener(NoProviderRedirect()).open(Request(VIDEOSCALE_BASE + path, data=body, headers=headers), timeout=30)
    except HTTPError as exc:
        if download_location and exc.code in {301, 302, 303, 307, 308}:
            return {'download_url': urljoin(VIDEOSCALE_BASE + path, exc.headers.get('Location', ''))}
        messages = {401: 'VideoScale rejected the API credentials', 403: 'VideoScale denied this API request',
                    402: 'VideoScale free allowance is exhausted; automatic purchases are disabled',
                    429: 'VideoScale is rate limiting requests; try again later'}
        raise RuntimeError(messages.get(exc.code, f'VideoScale API returned HTTP {exc.code}')) from None
    except (URLError, TimeoutError):
        raise RuntimeError('VideoScale API could not be reached; try again later') from None
    with response:
        raw = response.read(2 * 1024 * 1024 + 1)
    if len(raw) > 2 * 1024 * 1024:
        raise RuntimeError('VideoScale returned an oversized API response')
    try:
        return json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise RuntimeError('VideoScale did not return a JSON API response') from None


def select_audio_format(data):
    fields = provider_fields(data)
    formats = data if isinstance(data, list) else fields.get('formats')
    if not isinstance(formats, list):
        formats = provider_fields(fields.get('data')).get('formats')
    candidates = []
    for raw in formats if isinstance(formats, list) else []:
        item = provider_fields(raw)
        format_id = str(item.get('format_id', ''))
        if not re.fullmatch(r'[A-Za-z0-9_.-]{1,80}', format_id):
            continue
        audio_only = (item.get('vcodec') == 'none' and item.get('acodec') not in {None, '', 'none'})
        audio_only = audio_only or (item.get('has_video') is False and item.get('has_audio') is True)
        audio_only = audio_only or 'audio only' in str(item.get('format_note', '')).lower()
        if audio_only:
            bitrate = item.get('abr') or item.get('tbr') or 0
            candidates.append((float(bitrate) if isinstance(bitrate, (int, float)) else 0, format_id))
    if not candidates:
        provider_shape('formats', data)
        raise RuntimeError('VideoScale returned no supported audio-only formats')
    return max(candidates)[1]


def provider_task_id(data):
    fields = provider_fields(data)
    for candidate in (fields, provider_fields(fields.get('data'))):
        value = candidate.get('task_id') or candidate.get('taskid') or candidate.get('id')
        if isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_-]{1,160}', value):
            return value
    provider_shape('create', data)
    raise RuntimeError('VideoScale task response has an unsupported structure; no automatic resubmission was made')


def provider_download_url(data):
    if isinstance(data, str) and data.startswith('https://'):
        return data
    fields = provider_fields(data)
    for candidate in (fields, provider_fields(fields.get('data')), provider_fields(fields.get('result'))):
        value = candidate.get('download_url') or candidate.get('downloadurl') or candidate.get('url')
        if isinstance(value, str) and value.startswith('https://'):
            return value
    provider_shape('download', data)
    raise RuntimeError('VideoScale finished but returned no supported download URL')


def validate_provider_download(url):
    parts = urlsplit(url)
    if parts.scheme != 'https' or parts.username or parts.password or not parts.hostname:
        raise ValueError('VideoScale returned an invalid download location')
    if parts.port not in {None, 443}:
        raise ValueError('VideoScale returned an unsupported download port')
    try:
        addresses = socket.getaddrinfo(parts.hostname, 443, type=socket.SOCK_STREAM)
    except OSError:
        raise RuntimeError('VideoScale download host could not be resolved') from None
    if not addresses or any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
        raise ValueError('VideoScale returned a non-public download location')
    return url


class ProviderFileRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_provider_download(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def download_provider_file(url, destination):
    validate_provider_download(url)
    request = Request(url, headers={'User-Agent': 'WaveMusic/2'})
    total = 0
    deadline = time.monotonic() + 90
    with build_opener(ProviderFileRedirect()).open(request, timeout=30) as response:
        length = response.headers.get('Content-Length')
        if length and int(length) >= MAX_BYTES:
            raise ValueError('VideoScale track exceeds the audio size limit')
        with destination.open('wb') as output:
            while chunk := response.read(65536):
                total += len(chunk)
                if total >= MAX_BYTES or time.monotonic() > deadline:
                    raise ValueError('VideoScale download exceeded its size or time limit')
                output.write(chunk)
    if total < 100:
        raise ValueError('VideoScale returned no playable audio')


def videoscale_convert(url, target, ffmpeg):
    task_file = target.with_suffix('.provider.json')
    task_id = None
    if task_file.exists() and time.time() - task_file.stat().st_mtime < TTL:
        try:
            task_id = provider_task_id(json.loads(task_file.read_text()))
        except (ValueError, RuntimeError):
            pass
    if not task_id:
        formats = videoscale_request('/api/formats?' + urlencode({'video_url': url}))
        format_id = select_audio_format(formats)
        created = videoscale_request('/api/download', {'url': url, 'format_id': format_id,
            'postprocessors': [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '160'}]})
        task_id = provider_task_id(created)
        task_file.write_text(json.dumps({'task_id': task_id}))
        for old in target.parent.glob('*.provider.json'):
            if time.time() - old.stat().st_mtime >= TTL:
                old.unlink(missing_ok=True)
    deadline = time.monotonic() + 180
    while True:
        status = videoscale_request('/api/status/' + task_id)
        fields = provider_fields(status)
        data = provider_fields(fields.get('data')) or fields
        state = str(data.get('status') or data.get('state') or '').lower()
        if state in {'complete', 'completed', 'finished', 'success', 'done'}:
            break
        if state in {'failed', 'failure', 'error', 'cancelled', 'canceled'}:
            raise RuntimeError('VideoScale could not convert this track')
        if not state:
            provider_shape('status', status)
            raise RuntimeError('VideoScale returned an unsupported task-status structure; the existing task was retained')
        if time.monotonic() >= deadline:
            raise RuntimeError('VideoScale conversion is still pending; the existing task will be reused on retry')
        time.sleep(2)
    location = provider_download_url(videoscale_request('/api/download/' + task_id, download_location=True))
    source = target.with_suffix('.provider.audio')
    partial = target.with_suffix('.part.mp3')
    try:
        download_provider_file(location, source)
        with source.open('rb') as audio:
            prefix = audio.read(3)
        if prefix.startswith(b'ID3') or (len(prefix) >= 2 and prefix[0] == 255 and prefix[1] & 224 == 224):
            os.replace(source, target)
        else:
            run_process([ffmpeg, '-nostdin', '-hide_banner', '-loglevel', 'error', '-y', '-threads', '1',
                         '-i', str(source), '-vn', '-t', '7200', '-c:a', 'libmp3lame', '-b:a', '160k',
                         '-fs', str(MAX_BYTES), str(partial)], 90)
            if not partial.exists() or not 100 <= partial.stat().st_size < MAX_BYTES:
                raise ValueError('VideoScale did not produce a playable MP3')
            os.replace(partial, target)
    finally:
        source.unlink(missing_ok=True)
        partial.unlink(missing_ok=True)


def convert(url, target, ffmpeg):
    if urlsplit(url).hostname == 'www.youtube.com' and os.environ.get('VIDEOSCALE_API_USERNAME') and os.environ.get('VIDEOSCALE_API_PASSWORD'):
        return videoscale_convert(url, target, ffmpeg)
    info = extract_metadata(url)
    if info.get('is_live') or info.get('live_status') == 'is_live':
        raise ValueError('Live streams are not supported')
    if (info.get('duration') or 0) > 7200:
        raise ValueError('Track exceeds the two-hour preparation limit')
    audio = info.get('url', '')
    parts = urlsplit(audio)
    host = parts.hostname or ''
    youtube = urlsplit(url).hostname == 'www.youtube.com'
    valid_host = (host.endswith('.googlevideo.com') if youtube else
                  host == 'sndcdn.com' or host.endswith('.sndcdn.com')
                  or host == 'soundcloud.com' or host.endswith('.soundcloud.com')
                  or host == 'soundcloud.cloud' or host.endswith('.soundcloud.cloud'))
    if (parts.scheme != 'https' or parts.username or parts.password
            or not valid_host):
        raise ValueError('Extractor did not return an allowed provider audio stream')
    partial = target.with_suffix('.part.mp3')
    metadata_file = target.with_suffix('.info.json')
    source_template = str(target.with_suffix('.source.%(ext)s'))
    try:
        # Native HLS downloads fetch independent segments concurrently.
        metadata_file.write_text(json.dumps(info), encoding='utf-8')
        run_process([
            sys.executable, '-m', 'yt_dlp', '--ignore-config', '--no-playlist',
            '--load-info-json', str(metadata_file), '--concurrent-fragments', '3',
            '--hls-prefer-native', '--fixup', 'never', '--no-part', '--no-mtime',
            '--socket-timeout', '15', '--retries', '1', '--fragment-retries', '1',
            '--abort-on-unavailable-fragments', '--max-filesize', str(MAX_BYTES),
            '--output', source_template,
        ], 120)
        sources = list(target.parent.glob(target.stem + '.source.*'))
        if len(sources) != 1:
            raise ValueError('Audio download did not produce a complete file')
        codec = ['-c:a', 'copy'] if info.get('acodec') == 'mp3' else ['-c:a', 'libmp3lame', '-b:a', '160k']
        run_process([
            ffmpeg, '-nostdin', '-hide_banner', '-loglevel', 'error', '-y', '-threads', '1',
            '-i', str(sources[0]), '-vn', '-t', '7200', *codec,
            '-fs', str(MAX_BYTES), str(partial),
        ], 90)
        if not partial.exists() or partial.stat().st_size < 100:
            raise ValueError('Provider returned no playable audio')
        if partial.stat().st_size >= MAX_BYTES:
            raise ValueError('Track exceeds the audio size limit')
        os.replace(partial, target)
    finally:
        partial.unlink(missing_ok=True)
        metadata_file.unlink(missing_ok=True)
        for path in target.parent.glob(target.stem + '.source.*'):
            path.unlink(missing_ok=True)


@dataclass
class Job:
    id: str
    path: Path
    status: str = 'pending'
    error: str | None = None
    used: float = 0

    def public(self):
        return {'id': self.id, 'status': self.status, 'error': self.error}


class AudioStore:
    def __init__(self, root, ffmpeg, converter=convert):
        self.root, self.ffmpeg, self.converter = Path(root), ffmpeg, converter
        self.jobs = {}
        self.lock = Lock()
        self.workers = ThreadPoolExecutor(max_workers=1, thread_name_prefix='audio')
        self.root.mkdir(parents=True, exist_ok=True)
        for path in self.root.glob('*.mp3'):
            if re.fullmatch(r'[a-f0-9]{64}', path.stem) and 100 <= path.stat().st_size < MAX_BYTES:
                age = max(0, time.time() - path.stat().st_mtime)
                self.jobs[path.stem] = Job(path.stem, path, 'ready', used=time.monotonic() - age)
        self.prune()

    def prune(self):
        now = time.monotonic()
        completed = sorted((j for j in self.jobs.values() if j.status != 'pending'), key=lambda j: j.used)
        total = sum(j.path.stat().st_size for j in completed if j.path.exists())
        for job in completed:
            if now - job.used <= TTL and len(self.jobs) < MAX_JOBS and total <= MAX_CACHE:
                continue
            if job.path.exists():
                total -= job.path.stat().st_size
                job.path.unlink()
            del self.jobs[job.id]

    def prepare(self, url):
        url = normalize_url(url)
        key = sha256(url.encode()).hexdigest()
        with self.lock:
            job = self.jobs.get(key)
            if job and not (job.status == 'error' and time.monotonic() - job.used > 30):
                job.used = time.monotonic()
                if job.status == 'ready':
                    os.utime(job.path, None)
                return job.public()
            if job:
                del self.jobs[key]
            self.prune()
            if len(self.jobs) >= MAX_JOBS:
                raise RuntimeError('Audio queue is full; retry shortly')
            job = Job(key, self.root / (key + '.mp3'), used=time.monotonic())
            self.jobs[key] = job
            self.workers.submit(self.finish, job, url)
            return job.public()

    def finish(self, job, url):
        try:
            self.converter(url, job.path, self.ffmpeg)
            status, error = 'ready', None
        except subprocess.TimeoutExpired:
            status, error = 'error', 'Audio preparation timed out'
        except Exception as exc:
            status, error = 'error', str(exc)[:300]
        with self.lock:
            job.status, job.error, job.used = status, error, time.monotonic()

    def status(self, key):
        with self.lock:
            job = self.jobs.get(key)
            if not job:
                raise KeyError('Audio job expired; select the track again')
            job.used = time.monotonic()
            if job.status == 'ready':
                os.utime(job.path, None)
            return job.public()

    def audio(self, key):
        with self.lock:
            job = self.jobs.get(key)
            if not job or job.status != 'ready':
                raise KeyError('Audio is not ready')
            job.used = time.monotonic()
            os.utime(job.path, None)
            # Open before releasing the lock so eviction cannot race the response.
            return job.path.open('rb'), job.path.stat().st_size

    def close(self):
        self.workers.shutdown(wait=True)


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        allowed = getattr(self.server, 'allowed_hosts', ALLOWED_HOSTS)
        if self.headers.get('Host') not in allowed:
            return self.reply(403, {'error': 'Invalid bridge hostname'})
        if self.headers.get('Origin') or self.headers.get('Sec-Fetch-Site') == 'cross-site':
            return self.reply(403, {'error': 'Browser requests are disabled'})
        route = urlsplit(self.path)
        query = parse_qs(route.query)
        store = self.server.store
        token = getattr(self.server, 'token', BRIDGE_TOKEN)
        if route.path != '/health' and token:
            received = self.headers.get('Authorization', '')
            if not hmac.compare_digest(received.encode(), ('Bearer ' + token).encode()):
                return self.reply(401, {'error': 'Bridge access key is missing or invalid'})
        client = self.client_address[0]
        if os.environ.get('TRUST_PROXY') == '1':
            client = self.headers.get('CF-Connecting-IP') or self.headers.get('X-Real-IP') or client
        if not RATE_LIMIT.allow(client, route.path):
            return self.reply(429, {'error': 'Too many requests; retry in a minute'})
        try:
            if route.path == '/health':
                configured = bool(os.environ.get('VIDEOSCALE_API_USERNAME') and os.environ.get('VIDEOSCALE_API_PASSWORD'))
                return self.reply(200, {'status': 'ok', 'providers': ['soundcloud', 'youtube'], 'version': 3,
                                       'youtubeEngine': 'videoscale' if configured else 'yt-dlp'})
            if route.path == '/search':
                return self.reply(200, youtube_search(query.get('q', [''])[0],
                    int(query.get('limit', ['10'])[0]), int(query.get('offset', ['0'])[0])))
            if route.path == '/prepare':
                return self.reply(200, store.prepare(query.get('url', [''])[0]))
            key = query.get('id', [''])[0]
            if route.path == '/status':
                return self.reply(200, store.status(key))
            if route.path == '/audio':
                stream, size = store.audio(key)
                with stream:
                    self.send_response(200)
                    self.send_header('Content-Type', 'audio/mpeg')
                    self.send_header('Content-Length', str(size))
                    self.end_headers()
                    while chunk := stream.read(65536):
                        self.wfile.write(chunk)
                return
            self.reply(404, {'error': 'Unknown endpoint'})
        except ValueError as exc:
            self.reply(400, {'error': str(exc)})
        except KeyError as exc:
            self.reply(404, {'error': str(exc)})
        except RuntimeError as exc:
            self.reply(503, {'error': str(exc)})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, fmt, *args):
        pass


def main():
    import imageio_ffmpeg
    import yt_dlp  # Fail early if setup has not run.
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    if BIND_HOST not in {'127.0.0.1', 'localhost', '::1'} and len(BRIDGE_TOKEN) < 32:
        raise SystemExit('Remote binding requires BRIDGE_TOKEN with at least 32 characters')
    cache_dir = os.environ.get('CACHE_DIR')
    cache_context = nullcontext(cache_dir) if cache_dir else TemporaryDirectory(prefix='calcium_soundcloud_')
    with cache_context as cache:
        store = AudioStore(cache, ffmpeg)
        server = ThreadingHTTPServer((BIND_HOST, PORT), Handler)
        server.store = store
        print(f'SoundCloud bridge ready: http://{BIND_HOST}:{PORT}', flush=True)
        print('Keep this process running while using player.lua. Ctrl+C stops it.', flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
            store.close()


if __name__ == '__main__':
    main()
