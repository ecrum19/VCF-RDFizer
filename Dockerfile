ARG RMLSTREAMER_VERSION=2.5.0
ARG HDTC_VERSION=1.1.0

FROM eclipse-temurin:11-jre AS build-hdt-cpp

ARG RMLSTREAMER_VERSION

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    autoconf \
    automake \
    build-essential \
    ca-certificates \
    git \
    libserd-dev \
    libtool \
    pkg-config \
    zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
RUN git clone --depth 1 https://github.com/rdfhdt/hdt-cpp.git \
  && (git clone --branch "v${RMLSTREAMER_VERSION}" --depth 1 https://github.com/RMLio/RMLStreamer.git /opt/RMLStreamer \
      || git clone --branch "${RMLSTREAMER_VERSION}" --depth 1 https://github.com/RMLio/RMLStreamer.git /opt/RMLStreamer \
      || git clone --depth 1 https://github.com/RMLio/RMLStreamer.git /opt/RMLStreamer)

WORKDIR /opt/hdt-cpp
RUN ./autogen.sh \
  && ./configure \
  && make -j"$(nproc)" \
  && make install

RUN mkdir -p /opt/third_party_licenses \
  && cp /opt/hdt-cpp/libhdt/COPYRIGHT /opt/third_party_licenses/HDT-CPP.COPYRIGHT \
  && cp /opt/RMLStreamer/LICENSE /opt/third_party_licenses/RMLStreamer.LICENSE


FROM rust:1.93-slim AS build-hdtc

ARG HDTC_VERSION

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    libbz2-dev \
    liblzma-dev \
    pkg-config \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
RUN git clone --branch "v${HDTC_VERSION}" --depth 1 \
    https://github.com/frink-okn/hdtc.git

WORKDIR /opt/hdtc
RUN cargo build --locked --release \
  && mkdir -p /opt/third_party_licenses \
  && cp LICENSE /opt/third_party_licenses/HDTC.LICENSE


FROM eclipse-temurin:11-jre

ARG RMLSTREAMER_VERSION

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash \
    brotli \
    ca-certificates \
    coreutils \
    curl \
    findutils \
    gawk \
    gzip \
    libbz2-1.0 \
    liblzma5 \
    libserd-0-0 \
    nodejs \
    python3 \
    python3-venv \
    time \
  && rm -rf /var/lib/apt/lists/*

# Keep the DuckDB SQL dialect used by the disk-backed COTTAS merge stable.
# pycottas 1.1.0 accepts DuckDB >=1.2.2,<2, but leaving it unpinned makes a
# rebuild (or a cached layer) silently select a different Parquet COPY
# implementation.  The adapter is exercised against 1.5.5.
RUN python3 -m venv /opt/pycottas-venv \
  && /opt/pycottas-venv/bin/pip install --no-cache-dir \
    pycottas==1.1.0 \
    duckdb==1.5.5

RUN mkdir -p /opt/rmlstreamer \
  && curl -fsSL \
    -o /opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar \
    https://github.com/RMLio/RMLStreamer/releases/download/v${RMLSTREAMER_VERSION}/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar

COPY --from=build-hdt-cpp /usr/local/bin/rdf2hdt /usr/local/bin/rdf2hdt
COPY --from=build-hdt-cpp /usr/local/bin/hdt2rdf /usr/local/bin/hdt2rdf
COPY --from=build-hdt-cpp /usr/local/lib/libcds* /usr/local/lib/
COPY --from=build-hdt-cpp /usr/local/lib/libhdt* /usr/local/lib/
COPY --from=build-hdt-cpp /opt/third_party_licenses/ /usr/share/licenses/vcf-rdfizer/
COPY --from=build-hdtc /opt/hdtc/target/release/hdtc /usr/local/bin/hdtc
COPY --from=build-hdtc /opt/third_party_licenses/ /usr/share/licenses/vcf-rdfizer/
COPY THIRD_PARTY_NOTICES.md /usr/share/licenses/vcf-rdfizer/THIRD_PARTY_NOTICES.md
COPY src/*.sh /opt/vcf-rdfizer/
COPY src/*.py /opt/vcf-rdfizer/

RUN chmod +x /opt/vcf-rdfizer/*.sh \
  && chmod +x /usr/local/bin/rdf2hdt \
  && chmod +x /usr/local/bin/hdt2rdf \
  && chmod +x /usr/local/bin/hdtc

ENV RMLSTREAMER_JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar
ENV JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar
ENV HDTC_BIN=/usr/local/bin/hdtc
ENV HDT_INDEX_MEMORY_LIMIT=512M
ENV HDT_MERGE_MEMORY_LIMIT=512M
ENV COTTAS_MERGE_MEMORY_LIMIT=512M
ENV COTTAS_MERGE_THREADS=1
ENV RDF2HDT_BIN=/usr/local/bin/rdf2hdt
ENV HDT2RDF_BIN=/usr/local/bin/hdt2rdf
ENV COTTAS_PYTHON_BIN=/opt/pycottas-venv/bin/python
ENV LD_LIBRARY_PATH=/usr/local/lib

# COTTAS creates a temporary DuckDB database in the container working
# directory. The wrapper runs containers as the host UID/GID, so this path
# must be writable without requiring root or creating root-owned host files.
RUN mkdir -p /work && chmod 1777 /work
WORKDIR /work
