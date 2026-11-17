FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 --gid audio audio \
    && mkdir /data && chown audio:audio /data
COPY bridge.py .
USER audio
ENV PYTHONUNBUFFERED=1 BIND_HOST=0.0.0.0 PORT=10000 CACHE_DIR=/data
EXPOSE 10000
CMD ["python", "bridge.py"]
