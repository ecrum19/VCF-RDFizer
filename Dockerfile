ARG RMLSTREAMER_VERSION=2.5.0
ARG HDTC_VERSION=1.1.0
ARG COMUNICA_VERSION=5.3.0
# Comunica's HDT engine is versioned separately from its file engine and lags
# behind it. It provides native SPARQL over a .hdt artifact, so validation can
# query the compressed representation directly instead of decoding it first.
ARG COMUNICA_HDT_VERSION=5.0.1
# QLever is an optional second SPARQL engine for validation. Its binaries are
# copied from the upstream published image rather than built here: compiling
# QLever needs a large C++ toolchain and would dominate this image's build.
# Pin a digest or release tag here to make validation runs reproducible.
ARG QLEVER_IMAGE=adfreiburg/qlever:latest

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


# Named stage so the runtime image can COPY QLever's binaries out of it.
FROM ${QLEVER_IMAGE} AS qlever


FROM eclipse-temurin:11-jre

ARG RMLSTREAMER_VERSION

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    bash \
    bcftools \
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
    npm \
    python3 \
    python3-venv \
    raptor2-utils \
    time \
  && rm -rf /var/lib/apt/lists/*

# Keep the COTTAS conversion runtime and Parquet streaming-writer dialect
# stable. pycottas performs the per-chunk conversion; PyArrow performs the
# bounded-memory k-way merge of those already sorted chunks.
RUN python3 -m venv /opt/pycottas-venv \
  && /opt/pycottas-venv/bin/pip install --no-cache-dir \
    pycottas==1.1.0 \
    duckdb==1.5.5 \
    pyarrow==22.0.0 \
    numpy==2.4.6 \
    cyvcf2==0.34.0 \
    pyshacl==0.30.1

ARG COMUNICA_HDT_VERSION

# The HDT engine compiles native bindings, so a toolchain is needed at install
# time but not afterwards; it is purged in the same layer to keep it out of the
# image. `python3` is already present and is what node-gyp needs.
RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential \
  && npm install --global \
    "@comunica/query-sparql-file@${COMUNICA_VERSION}" \
    "@comunica/query-sparql-hdt@${COMUNICA_HDT_VERSION}" \
  && apt-get purge -y --auto-remove build-essential \
  && rm -rf /var/lib/apt/lists/*

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

# Optional QLever SPARQL engine (--validation-engine qlever). Comunica remains
# the default, so an image whose QLever binaries turn out to be unusable is
# still fully functional; the validator reports the reason instead of failing
# obscurely. Pin a different tag or digest with
# --build-arg QLEVER_IMAGE=adfreiburg/qlever:<tag>.
#
# QLever's image is built on a different Ubuntu release than this one, so the
# binaries alone are not enough: their Boost, ICU, jemalloc and io_uring
# sonames are release-specific and absent here. Those libraries travel with the
# binaries into a private directory that only QLever's own processes are
# pointed at, so they cannot shadow anything the rest of the image links
# against. (glibc itself is not copied - it is backward compatible, and this
# base is newer than QLever's.)
COPY --from=qlever /qlever/qlever-index /opt/qlever/bin/qlever-index
COPY --from=qlever /qlever/qlever-server /opt/qlever/bin/qlever-server
COPY --from=qlever \
  /lib/x86_64-linux-gnu/libboost_iostreams.so.1.83.0 \
  /lib/x86_64-linux-gnu/libboost_program_options.so.1.83.0 \
  /lib/x86_64-linux-gnu/libboost_url.so.1.83.0 \
  /lib/x86_64-linux-gnu/libgomp.so.1 \
  /lib/x86_64-linux-gnu/libicudata.so.74 \
  /lib/x86_64-linux-gnu/libicui18n.so.74 \
  /lib/x86_64-linux-gnu/libicuuc.so.74 \
  /lib/x86_64-linux-gnu/libjemalloc.so.2 \
  /lib/x86_64-linux-gnu/liburing.so.2 \
  /opt/qlever/lib/
COPY THIRD_PARTY_NOTICES.md /usr/share/licenses/vcf-rdfizer/THIRD_PARTY_NOTICES.md
COPY src/*.sh /opt/vcf-rdfizer/
COPY src/*.py /opt/vcf-rdfizer/
COPY src/validation/ /opt/vcf-rdfizer/validation/
# Shared, dependency-free helper used by both the host CLI and the in-container
# conversion runner, so it lives at the repository root rather than in src/.
COPY vcf_rdfizer_gzip.py /opt/vcf-rdfizer/

RUN chmod +x /opt/vcf-rdfizer/*.sh \
  && chmod +x /usr/local/bin/rdf2hdt \
  && chmod +x /usr/local/bin/hdt2rdf \
  && chmod +x /usr/local/bin/hdtc \
  && chmod +x /opt/qlever/bin/qlever-index /opt/qlever/bin/qlever-server

# QLever's binaries come from a different base image, so record at build time
# whether they actually link here. The validator reads this marker to explain
# an unavailable engine instead of surfacing a bare "not found".
RUN set -eu; \
  status="ok"; \
  for binary in qlever-index qlever-server; do \
    missing="$(LD_LIBRARY_PATH=/opt/qlever/lib ldd "/opt/qlever/bin/$binary" 2>&1 | grep 'not found' || true)"; \
    if [ -n "$missing" ]; then \
      status="QLever binary $binary has unresolved shared libraries in this base image: $missing"; \
      echo "WARNING: $status" >&2; \
    fi; \
  done; \
  printf '%s\n' "$status" > /opt/vcf-rdfizer/qlever-status.txt

ENV RMLSTREAMER_JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar
ENV JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar
ENV HDTC_BIN=/usr/local/bin/hdtc
ENV HDT_INDEX_MEMORY_LIMIT=512M
ENV HDT_MERGE_MEMORY_LIMIT=512M
ENV COTTAS_MERGE_BATCH_ROWS=2048
ENV RDF2HDT_BIN=/usr/local/bin/rdf2hdt
ENV HDT2RDF_BIN=/usr/local/bin/hdt2rdf
ENV COTTAS_PYTHON_BIN=/opt/pycottas-venv/bin/python
ENV QLEVER_INDEX_BUILDER_BIN=/opt/qlever/bin/qlever-index
ENV QLEVER_SERVER_BIN=/opt/qlever/bin/qlever-server
ENV LD_LIBRARY_PATH=/usr/local/lib

# COTTAS creates a temporary DuckDB database in the container working
# directory. The wrapper runs containers as the host UID/GID, so this path
# must be writable without requiring root or creating root-owned host files.
RUN mkdir -p /work && chmod 1777 /work
WORKDIR /work
