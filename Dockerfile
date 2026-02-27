# Use ARGs for versions
ARG SAMTOOLS_VERSION="1.23"
ARG FALCO_VERSION="1.2.3"
ARG PYTHON_VERSION_FOR_APP="3.13"

# ----------- Builder Stage (Heavy) -----------
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

ARG SAMTOOLS_VERSION
ARG FALCO_VERSION
ARG PYTHON_VERSION_FOR_APP

ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies + binutils for stripping
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get -y --no-install-recommends install \
    ca-certificates wget curl bzip2 unzip make gcc g++ pkg-config \
    zlib1g-dev libxml2-dev libbz2-dev liblzma-dev \
    git binutils && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# --- Create Isolated Environments ---
ENV APP_VENV_PATH=/opt/app_venv
RUN python${PYTHON_VERSION_FOR_APP} -m venv ${APP_VENV_PATH}

# Core libraries
ENV CORE_PACKAGES="polars==1.38.1 scipy==1.17.1 numpy==2.4.2 pysam==0.23.3 pyyaml"
RUN uv pip install --python ${APP_VENV_PATH}/bin/python --no-cache ${CORE_PACKAGES}

# CLI tools (using isolated tool-dirs)
ENV UV_TOOL_BIN_DIR=/usr/local/bin
ENV UV_TOOL_DIR=/opt/uv_tools
RUN uv tool install multiqc==1.33 --no-cache && \
    uv tool install snakemake==9.16.3 --no-cache && \
    uv tool install cutseq==0.0.68 --no-cache && \
    uv tool install markdup==0.0.25 --no-cache && \
    uv tool install countmut==0.0.8 --no-cache && \
    uv tool install coralsnake==0.0.177 --no-cache

# --- Build & STRIP samtools/bgzip ---
WORKDIR /build/sources
COPY samtools-1.23.tar.bz2 htslib-1.23.tar.bz2 ./
RUN tar -xjvf samtools-1.23.tar.bz2 && cd samtools-1.23 && ./configure --without-curses && make -j$(nproc) samtools && strip samtools && mv samtools /usr/local/bin/ && cd .. && \
    tar -xjvf htslib-1.23.tar.bz2 && cd htslib-1.23 && ./configure && make -j$(nproc) bgzip && strip bgzip && mv bgzip /usr/local/bin/

# --- Build & STRIP hisat-3n ---
COPY hisat2-hisat-3n.tar.gz .
RUN tar -xzvf hisat2-hisat-3n.tar.gz && cd hisat2-* && make -j$(nproc) && \
    strip hisat2-align-s hisat2-align-l hisat2-build-s hisat2-build-l && \
    mv hisat-3n hisat2-align-s hisat2-align-l hisat-3n-build hisat2-build-s hisat2-build-l /usr/local/bin/

# --- Build & STRIP Falco ---
COPY falco-1.2.3.tar.gz .
RUN tar -xzvf falco-1.2.3.tar.gz && cd falco-* && ./configure && make -j$(nproc) && strip falco && mv falco /usr/local/bin/

# --- CLEANUP Python environments ---
RUN find /opt -name "__pycache__" -type d -exec rm -rf {} + && \
    find /opt -name "*.pyc" -delete && \
    find /opt -name "tests" -type d -exec rm -rf {} + && \
    find /opt -name "docs" -type d -exec rm -rf {} +


# ----------- Final Stage (Minimal Runtime) -----------
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS final

ENV DEBIAN_FRONTEND=noninteractive
ENV PIPELINE_HOME=/pipeline
ENV APP_VENV_PATH=/opt/app_venv
ENV UV_TOOL_DIR=/opt/uv_tools

# Minimal runtime path
ENV PATH="${APP_VENV_PATH}/bin:/usr/local/bin:$PATH"

# Install only essential shared libraries
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && \
    apt-get -y --no-install-recommends install \
    ca-certificates zlib1g libxml2 libbz2-1.0 liblzma5 pigz && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy only the necessary directories from builder
COPY --from=builder /opt /opt
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application logic
COPY ./Snakefile ./default.yaml ./entrypoint ./VERSION ${PIPELINE_HOME}/
COPY ./src/ ${PIPELINE_HOME}/src/

WORKDIR /workspace
RUN chmod +x ${PIPELINE_HOME}/entrypoint
ENTRYPOINT ["/pipeline/entrypoint"]
