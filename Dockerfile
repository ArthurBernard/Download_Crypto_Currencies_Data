# dccd v3 — daemon + web UI image.
#
#   docker build -t dccd .
#   docker run --rm -p 8080:8080 \
#       -v "$PWD/config.yml:/etc/dccd/config.yml:ro" \
#       -v dccd-data:/data dccd
#
# The config's settings.data_path should point at /data (the mounted volume),
# and ui_host must be 0.0.0.0 to be reachable from outside the container.
#
# Old CPUs without AVX2 (e.g. pre-Haswell): the default polars wheel crashes with
# SIGILL. Build with the LTS-CPU variant instead:
#   docker build --build-arg POLARS_VARIANT=polars-lts-cpu -t dccd .
#
# Base image pinned to a digest for reproducible builds (tag: python:3.12-slim).
FROM python:3.12-slim@sha256:090ba77e2958f6af52a5341f788b50b032dd4ca28377d2893dcf1ecbdfdfe203 AS base

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    XDG_CONFIG_HOME=/etc

WORKDIR /app

# polars (default, AVX2) or polars-lts-cpu (older CPUs). See header.
ARG POLARS_VARIANT=polars

# Install dependencies first for better layer caching.
COPY pyproject.toml README.md ./
COPY dccd ./dccd
# polars-lts-cpu lags the latest polars release, so the variant is installed
# unpinned (newest available that runs on the older CPU) — not pinned to polars's
# version, which would fail to resolve.
RUN pip install ".[daemon,ui]" \
 && if [ "$POLARS_VARIANT" != "polars" ]; then \
        pip uninstall -y polars \
     && pip install "${POLARS_VARIANT}"; \
    fi

# rclone is only needed if storage.remotes is configured; install on demand.
VOLUME ["/data"]
EXPOSE 8080

# Config is mounted at /etc/dccd/config.yml (XDG_CONFIG_HOME=/etc).
ENTRYPOINT ["dccd"]
CMD ["start", "--host", "0.0.0.0", "--port", "8080"]
