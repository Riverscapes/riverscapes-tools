FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /src

COPY pyproject.toml .
COPY README.md .
COPY LICENSE .
COPY packages ./packages
COPY lib ./lib

# Build wheels for dependencies once and drop the project wheel;
# we'll install the source package in editable mode later so that
# JSON/config assets stay available straight from the workspace.
RUN pip install --upgrade pip setuptools wheel \
    && pip wheel --wheel-dir /tmp/wheels . \
    && rm -f /tmp/wheels/riverscapes_tools-*.whl


FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        ca-certificates \
        curl \
        unzip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/riverscapes-tools
COPY packages ./packages
COPY lib ./lib
COPY pyproject.toml .
COPY README.md .
COPY LICENSE .

COPY --from=builder /tmp/wheels /tmp/wheels
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir /tmp/wheels/* \
    && pip install --no-cache-dir --no-deps -e . \
    && rm -rf /tmp/wheels

RUN useradd --create-home --shell /bin/bash rsuser \
    && chown -R rsuser:rsuser /opt/riverscapes-tools

USER rsuser
WORKDIR /workspace

ENV PYTHONPATH=/opt/riverscapes-tools/packages:/opt/riverscapes-tools/lib/commons

ENTRYPOINT ["python", "-m", "rscontext.rs_context"]
CMD ["--help"]
