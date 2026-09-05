# Use ARGs for versions
ARG SAMTOOLS_VERSION="1.24"
ARG FALCO_VERSION="2.0.1"
ARG BWA_MEM2_VERSION="2.3"
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

# Registry host for the uv launcher image (ghcr.io). Override for a China
# ghcr mirror that serves the same astral-sh/uv image, e.g.:
#   China:        --build-arg UV_IMAGE=ghcr.nju.edu.cn
#   Rest of world (default): ghcr.io (GitHub Actions / Docker Hub friendly)
ARG UV_IMAGE="ghcr.io"

# ----------- UV launcher stage -----------
# BuildKit does NOT support variable expansion in COPY --from, so we define a
# dedicated stage whose FROM uses the ARG (FROM supports global-scope ARGs).
# The host is controlled by UV_IMAGE (default ghcr.io; set to a China ghcr
# mirror like ghcr.nju.edu.cn to build fast from CN).
FROM ${UV_IMAGE}/astral-sh/uv:latest AS uv

# ----------- Builder Stage (Heavy) -----------
FROM python:3.13-slim-bookworm AS builder

ARG SAMTOOLS_VERSION
ARG FALCO_VERSION
ARG BWA_MEM2_VERSION
ARG PYTHON_VERSION_FOR_APP
ARG UV_DEFAULT_INDEX
ARG APT_MIRROR
ARG GH_BASEURL
ARG UV_IMAGE

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

# Install uv (from the uv stage whose FROM host is controlled by UV_IMAGE)
COPY --from=uv /uv /uvx /usr/local/bin/

# --- Single merged runtime environment -----------------------------------
# All pipeline Python libraries + bioinformatics CLI tools live in ONE venv,
# instead of a separate per-tool venv (`uv tool install`). Heavy shared deps
# (pysam ~75 MB, numpy ~58 MB, rich/click, ...) are therefore installed once.
# /opt/app_venv is kept as a symlink so existing default.yaml / entrypoint
# paths keep working unchanged.
ENV VENV_PATH=/opt/venv

# Core libraries + CLI tools (all in one env)
# NOTE: MultiQC was removed/replaced by the lightweight src/report_html.py.
# `polars` (regular dist, pinned): the only remaining src/*.py consumer is
# filter_sites.py; coralsnake >= 0.2 also declares a hard `polars` dep, so
# pin the regular dist - installing polars-lts-cpu alongside it would give
# two distributions providing the same `polars` module.
# countmut >= 0.2.2: run_countmut emits the per-strand 2-group conversion
# view (chrom pos strand motif u0 u1 m0 m1) natively via the -e group router
# + --output-format template; the src/pileup_reformat.py bridge is retired.
# coralsnake 0.2.1: minus-transcript SEQ/QUAL liftover fix + t2g CIGAR fixes.
RUN python${PYTHON_VERSION_FOR_APP} -m venv ${VENV_PATH} && \
    uv pip install --python ${VENV_PATH}/bin/python --no-cache \
        snakemake==9.26.1 cutseq==0.0.70 markdup==0.0.29 \
        countmut==0.2.2 coralsnake==0.2.1 prismalign==0.2.11 \
        duckdb==1.5.5 polars==1.33.1 scipy==1.18.1 numpy==2.5.2 pysam==0.24.0 pyyaml==6.0.3 && \
    for t in snakemake cutseq markdup countmut coralsnake prismalign; do \
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

# --- Install bwa-mem2 prebuilt Linux binary (`--adapter bwa-mem2` path) ---
WORKDIR /build
RUN curl -L --http1.1 --retry 5 --retry-all-errors --retry-delay 5 ${GH_BASEURL}/bwa-mem2/bwa-mem2/releases/download/v${BWA_MEM2_VERSION}/bwa-mem2-${BWA_MEM2_VERSION}_x64-linux.tar.bz2 -o bwa-mem2.tar.bz2 && \
    tar -xjvf bwa-mem2.tar.bz2 --strip-components 1 -C /usr/local/bin && \
    chmod +x /usr/local/bin/bwa-mem2* && \
    rm -rf /build/bwa-mem2.tar.bz2

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
    ca-certificates zlib1g libbz2-1.0 liblzma5 liblua5.4-0 && \
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
