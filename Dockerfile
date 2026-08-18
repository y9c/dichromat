# Use ARGs for versions
ARG SAMTOOLS_VERSION="1.23"
ARG FALCO_VERSION="1.2.3"
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

# --- Create Isolated Environments ---
ENV APP_VENV_PATH=/opt/app_venv
RUN python${PYTHON_VERSION_FOR_APP} -m venv ${APP_VENV_PATH}

# Core libraries
ENV CORE_PACKAGES="polars==1.38.1 scipy==1.17.1 numpy==2.4.2 pysam==0.23.3 pyyaml"
RUN uv pip install --python ${APP_VENV_PATH}/bin/python --no-cache ${CORE_PACKAGES}

# CLI tools
ENV UV_TOOL_BIN_DIR=/usr/local/bin
ENV UV_TOOL_DIR=/opt/uv_tools
# Install python tools (Consolidated to save layers/space)
RUN uv tool install multiqc==1.33 --no-cache && \
    uv tool install snakemake==9.16.3 --no-cache && \
    uv tool install cutseq==0.0.68 --no-cache && \
    uv tool install markdup==0.0.27 --no-cache && \
    uv tool install countmut==0.0.8 --no-cache && \
    uv tool install coralsnake==0.0.210 --no-cache

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

# --- Build Falco ---
WORKDIR /build/falco
RUN curl -L --retry 5 --retry-all-errors --retry-delay 5 ${GH_BASEURL}/smithlabcode/falco/releases/download/v${FALCO_VERSION}/falco-${FALCO_VERSION}.tar.gz -o falco.tar.gz && \
    tar -xzvf falco.tar.gz && cd falco-* && ./configure && make -j$(nproc) && strip falco && \
    mv falco /usr/local/bin/ && cd .. && rm -rf /build/falco

# --- CLEANUP ---
RUN find /opt -name "__pycache__" -type d -exec rm -rf {} + && \
    find /opt -name "*.pyc" -delete


# ----------- Final Stage -----------
FROM python:3.13-slim-bookworm AS final

ENV DEBIAN_FRONTEND=noninteractive
ENV PIPELINE_HOME=/pipeline
ENV APP_VENV_PATH=/opt/app_venv
ENV UV_TOOL_DIR=/opt/uv_tools
ENV PATH="${APP_VENV_PATH}/bin:/usr/local/bin:$PATH"

ARG APT_MIRROR
RUN if [ -n "${APT_MIRROR}" ]; then \
        sed -i "s|deb.debian.org|${APT_MIRROR}|g" /etc/apt/sources.list.d/debian.sources; \
    fi && \
    apt-get update && \
    apt-get -y --no-install-recommends install \
    ca-certificates zlib1g libxml2 libbz2-1.0 liblzma5 pigz && \
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
