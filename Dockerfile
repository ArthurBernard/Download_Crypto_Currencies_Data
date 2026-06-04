# dccd v3 — daemon + web UI image.
#
#   docker build -t dccd .
#   docker run --rm -p 8080:8080 \
#       -v "$PWD/config.yml:/etc/dccd/config.yml:ro" \
#       -v dccd-data:/data dccd
#
# The config's settings.data_path should point at /data (the mounted volume),
# and ui_host must be 0.0.0.0 to be reachable from outside the container.
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    XDG_CONFIG_HOME=/etc

WORKDIR /app

# Install dependencies first for better layer caching.
COPY pyproject.toml README.md ./
COPY dccd ./dccd
RUN pip install ".[daemon,ui]"

# rclone is only needed if storage.remotes is configured; install on demand.
VOLUME ["/data"]
EXPOSE 8080

# Config is mounted at /etc/dccd/config.yml (XDG_CONFIG_HOME=/etc).
ENTRYPOINT ["dccd"]
CMD ["start", "--host", "0.0.0.0", "--port", "8080"]
