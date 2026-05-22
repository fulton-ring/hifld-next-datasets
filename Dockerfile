# syntax=docker/dockerfile:1.7
#
# User-code image for the Dagster Helm chart. The same image runs in three
# places once deployed:
#   1. The userDeployments Pod (`dagster api grpc -m dagster_hifld.definitions`)
#   2. Run pods spawned by K8sRunLauncher
#   3. (Optionally) step pods if k8s_job_executor is enabled later
#
# Heavy dependencies driven by the actual code:
#   - GDAL system libs for fiona/geopandas (src/dagster_hifld/conversion.py)
#   - tippecanoe binary for PMTiles generation
#   - uv-managed Python environment

# ---- Stage 1: install dependencies into a venv -------------------------------
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.9.0 AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        python3-pip \
        python3-venv \
        tippecanoe \
 && rm -rf /var/lib/apt/lists/*

# Install uv into a stable location.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install Python deps first (better layer caching). Use --no-install-project so
# the source-only sync doesn't require src/ to exist yet.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Install the project itself.
COPY src/ ./src/
COPY README.md ./
RUN uv sync --frozen --no-dev

# ---- Stage 2: runtime image --------------------------------------------------
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.9.0 AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:${PATH}" \
    DAGSTER_HOME=/opt/dagster/dagster_home

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        tippecanoe \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p ${DAGSTER_HOME}

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/pyproject.toml /app/pyproject.toml
COPY HIFLD_Open_Inventory_12112025.csv /app/HIFLD_Open_Inventory_12112025.csv

WORKDIR /app

EXPOSE 4000

# Default to the user-code gRPC server. The Helm chart overrides this for
# webserver and daemon Deployments via dagsterApiGrpcArgs.
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "dagster_hifld.definitions"]
