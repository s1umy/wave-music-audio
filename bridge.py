"""SoundCloud audio adapter with optional authenticated remote access."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import hmac
from collections import OrderedDict
from contextlib import nullcontext
import os
from pathlib import Path
import re
import subprocess
import sys
from tempfile import TemporaryDirectory
from threading import Lock
import time
from urllib.parse import parse_qs, urlsplit, urlunsplit

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
        limit = 30 if route == '/prepare' else 600
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
    segments = parts.path.strip('/').split('/')
    if (parts.scheme != 'https' or parts.hostname not in {'soundcloud.com', 'www.soundcloud.com'}
            or parts.username or parts.password or parts.port not in {None, 443}
            or len(segments) != 2 or not all(re.fullmatch(r'[\w-]+', s) for s in segments)
            or segments[0] in {'discover', 'search', 'charts', 'you', 'settings'}):
        raise ValueError('Expected a public https://soundcloud.com/artist/track URL')
    return urlunsplit(('https', 'soundcloud.com', '/' + '/'.join(segments), '', ''))


def run_process(args, timeout):
    result = subprocess.run(args, capture_output=True, text=True, encoding='utf-8',
                            errors='replace', timeout=timeout,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    if result.returncode:
        message = result.stderr.strip().splitlines()
        raise RuntimeError(message[-1][:300] if message else 'Audio preparation failed')
    return result.stdout


def convert(url, target, ffmpeg):
    metadata = run_process([
        sys.executable, '-m', 'yt_dlp', '--ignore-config', '--no-playlist',
        '--skip-download', '--dump-single-json', '--format', 'bestaudio[acodec=mp3]/bestaudio',
        '--socket-timeout', '15', '--retries', '1', '--', url,
    ], 55)
    info = json.loads(metadata)
    if (info.get('duration') or 0) > 7200:
        raise ValueError('Track exceeds the two-hour preparation limit')
    audio = info.get('url', '')
    parts = urlsplit(audio)
    host = parts.hostname or ''
    if (parts.scheme != 'https' or parts.username or parts.password
            or not (host == 'sndcdn.com' or host.endswith('.sndcdn.com')
                    or host == 'soundcloud.com' or host.endswith('.soundcloud.com')
                    or host == 'soundcloud.cloud' or host.endswith('.soundcloud.cloud'))):
        raise ValueError('Extractor did not return a SoundCloud audio stream')
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
            ffmpeg, '-nostdin', '-hide_banner', '-loglevel', 'error', '-y',
            '-i', str(sources[0]), '-vn', '-t', '7200', *codec,
            '-fs', str(MAX_BYTES), str(partial),
        ], 30)
        if not partial.exists() or partial.stat().st_size < 100:
            raise ValueError('SoundCloud returned no playable audio')
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
            status, error = 'error', 'SoundCloud audio preparation timed out'
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
                return self.reply(200, {'status': 'ok'})
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
