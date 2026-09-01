# Use ARGs for versions
ARG SAMTOOLS_VERSION="1.23"
ARG FALCO_VERSION="2.0.1"
ARG PYTHON_VERSION_FOR_APP="3.13"

# -------- Mirror toggles (China vs rest of world) --------
# PyPI index for uv.
#   China:        --build-arg UV_DEFAULT_INDEX=https://pypi.tuna.tsinghua.edu.cn/simple
#   Rest of world (default): official PyPI
ARG UV_DEFAULT_INDEX="https://pypi.org/simple"

# APT mirror. Empty string uses the official Debian sources.
#   China:        --build-arg APT_MIRROR=mirrors.aliyun.com
#   Rest of world (default): (empty) official Debian
ARG APT_MIRROR=""

# Base URL prefix for GitHub source downloads. Useful for GitHub mirrors/proxies.
#   China example (GitHub proxy): --build-arg GH_BASEURL=https://ghproxy.com/
#   Rest of world (default):      direct github.com
ARG GH_BASEURL="https://github.com"

# ----------- Builder Stage (Heavy) -----------
FROM python:3.13-slim-bookworm AS builder

ARG SAMTOOLS_VERSION
ARG FALCO_VERSION
ARG PYTHON_VERSION_FOR_APP
ARG UV_DEFAULT_INDEX
ARG APT_MIRROR
ARG GH_BASEURL

ENV DEBIAN_FRONTEND=noninteractive

# PyPI index for uv (build environment)
ENV UV_DEFAULT_INDEX=${UV_DEFAULT_INDEX}

# Install build dependencies
# Configure APT mirror conditionally (leave official if APT_MIRROR empty)
RUN if [ -n "${APT_MIRROR}" ]; then \
        sed -i "s|deb.debian.org|${APT_MIRROR}|g" /etc/apt/sources.list.d/debian.sources; \
    fi && \
    apt-get update && \
    apt-get -y --no-install-recommends install \
    ca-certificates wget curl bzip2 unzip make gcc g++ pkg-config \
    zlib1g-dev libxml2-dev libbz2-dev liblzma-dev \
    git binutils && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# --- Single merged runtime environment -----------------------------------
# All pipeline Python libraries + bioinformatics CLI tools live in ONE venv,
# instead of a separate per-tool venv (`uv tool install`). Heavy shared deps
# (pysam ~75 MB, numpy ~58 MB, rich/click, ...) are therefore installed once.
# /opt/app_venv is kept as a symlink so existing default.yaml / entrypoint
# paths keep working unchanged.
ENV VENV_PATH=/opt/venv

# Core libraries + CLI tools (all in one env)
# NOTE: MultiQC was removed/replaced by the lightweight src/report_html.py.
# polars-lts-cpu provides the `polars` module that src/*.py use directly
# (scan_csv/scan_parquet/sink_parquet/...).
RUN python${PYTHON_VERSION_FOR_APP} -m venv ${VENV_PATH} && \
    uv pip install --python ${VENV_PATH}/bin/python --no-cache \
        snakemake==9.16.3 cutseq==0.0.70 markdup==0.0.27 \
        countmut==0.0.8 coralsnake==0.0.210 \
        duckdb==1.5.5 polars-lts-cpu==1.33.1 scipy==1.17.1 numpy==2.4.2 pysam==0.23.3 pyyaml && \
    for t in snakemake cutseq markdup countmut coralsnake; do \
        ln -s ${VENV_PATH}/bin/$t /usr/local/bin/$t; \
    done && \
    ln -s ${VENV_PATH} /opt/app_venv

# --- Build samtools/bgzip ---
WORKDIR /build/sources
RUN curl -L --http1.1 --retry 5 --retry-all-errors --retry-delay 5 ${GH_BASEURL}/samtools/samtools/releases/download/${SAMTOOLS_VERSION}/samtools-${SAMTOOLS_VERSION}.tar.bz2 -o samtools.tar.bz2 && \
    tar -xjvf samtools.tar.bz2 --strip-components 1 && \
    ./configure --without-curses && \
    make -j$(nproc) samtools && \
    strip samtools && \
    mv samtools /usr/local/bin/ && \
    rm -rf *

RUN curl -L --http1.1 --retry 5 --retry-all-errors --retry-delay 5 ${GH_BASEURL}/samtools/htslib/releases/download/${SAMTOOLS_VERSION}/htslib-${SAMTOOLS_VERSION}.tar.bz2 -o htslib.tar.bz2 && \
    tar -xjvf htslib.tar.bz2 --strip-components 1 && \
    ./configure && \
    make -j$(nproc) bgzip && \
    strip bgzip && \
    mv bgzip /usr/local/bin/ && \
    rm -rf *

# --- Build hisat3n from GitHub release (v0.1.23) ---
WORKDIR /build/hisat2
RUN curl -L --retry 5 --retry-all-errors --retry-delay 5 ${GH_BASEURL}/y9c/hisat2/archive/refs/tags/v0.1.23.tar.gz -o hisat2.tar.gz && \
    tar -xzvf hisat2.tar.gz --strip-components 1 && \
    make -j$(nproc) hisat2-align-s hisat2-build-s hisat2-inspect-s EXTRA_FLAGS="-static-libstdc++ -static-libgcc -mavx2" && \
    g++ -O3 -o hisat3n hisat2_wrapper.cpp -static-libstdc++ -static-libgcc && \
    strip hisat2-align-s hisat2-build-s hisat2-inspect-s hisat3n && \
    mv hisat2-align-s hisat2-build-s hisat2-inspect-s hisat3n /usr/local/bin/ && \
    ln -s /usr/local/bin/hisat3n /usr/local/bin/hisat-3n && \
    rm -rf /build/hisat2

# --- Install falco (FastQC-style QC; v2+ ships as a prebuilt linux binary -
# no source build needed; x86-64, matching the ubuntu build runners) ---
WORKDIR /build/falco
RUN curl -L --retry 5 --retry-all-errors --retry-delay 5 ${GH_BASEURL}/smithlabcode/falco/releases/download/v${FALCO_VERSION}/falco-${FALCO_VERSION}-Linux.tar.gz -o falco.tar.gz && \
    tar -xzf falco.tar.gz --strip-components 2 -C /usr/local/bin falco-${FALCO_VERSION}-Linux/bin/falco && \
    chmod +x /usr/local/bin/falco && \
    rm -rf /build/falco

# --- CLEANUP ---
# Drop bytecode and test/docs trees, and remove the uv/uvx launchers which are
# only needed at build time but would otherwise be copied into the final
# image.
# NOTE: we deliberately do NOT strip *.so files - wheels are already stripped,
# and `strip` breaks page alignment of scipy's bundled OpenBLAS
# (libscipy_openblas64_*.so) so `import numpy/scipy` fails at runtime
# (caught by the SIF smoke-test).
# pulp is a snakemake transitive dep we don't use (default scheduler); the
# kaleido/pyarrow entries are now empty since MultiQC was removed - kept as a
# defensive catch-all.
RUN find /opt -name "__pycache__" -type d -exec rm -rf {} + && \
    find /opt -name "*.pyc" -delete && \
    (find ${VENV_PATH}/lib -maxdepth 4 -type d \( -iname 'kaleido*' -o -iname 'pyarrow*' -o -iname 'pulp*' \) -exec rm -rf {} + 2>/dev/null || true) && \
    (find /opt -type d \( -name tests -o -name test -o -name docs -o -name doc -o -name examples \) -prune -exec rm -rf {} + 2>/dev/null || true) && \
    rm -f /usr/local/bin/uv /usr/local/bin/uvx


# ----------- Final Stage -----------
FROM python:3.13-slim-bookworm AS final

ENV DEBIAN_FRONTEND=noninteractive
ENV PIPELINE_HOME=/pipeline
ENV APP_VENV_PATH=/opt/app_venv
ENV PATH="${APP_VENV_PATH}/bin:/usr/local/bin:$PATH"

ARG APT_MIRROR
RUN if [ -n "${APT_MIRROR}" ]; then \
        sed -i "s|deb.debian.org|${APT_MIRROR}|g" /etc/apt/sources.list.d/debian.sources; \
    fi && \
    apt-get update && \
    apt-get -y --no-install-recommends install \
    ca-certificates zlib1g libbz2-1.0 liblzma5 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy only necessary folders/bins from builder to keep size minimal
COPY --from=builder /opt /opt
COPY --from=builder /usr/local/bin /usr/local/bin

WORKDIR ${PIPELINE_HOME}
COPY ./src/ ${PIPELINE_HOME}/src/
COPY ./Snakefile ./default.yaml ./entrypoint ./VERSION ${PIPELINE_HOME}/

RUN chmod +x ${PIPELINE_HOME}/entrypoint

WORKDIR /workspace
ENTRYPOINT ["/pipeline/entrypoint"]
