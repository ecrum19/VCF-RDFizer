ARG RMLSTREAMER_VERSION=2.5.0
ARG HDT_JAVA_PACKAGE_VERSION=3.0.10

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


FROM eclipse-temurin:11-jre

ARG RMLSTREAMER_VERSION
ARG HDT_JAVA_PACKAGE_VERSION

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
    libserd-0-0 \
    nodejs \
    python3 \
    python3-venv \
    time \
  && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/pycottas-venv \
  && /opt/pycottas-venv/bin/pip install --no-cache-dir pycottas

RUN mkdir -p /opt/rmlstreamer \
  && curl -fsSL \
    -o /opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar \
    https://github.com/RMLio/RMLStreamer/releases/download/v${RMLSTREAMER_VERSION}/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar

RUN mkdir -p /opt/hdt-java \
  && curl -fsSL \
    -o /tmp/hdt-java-package.tar.gz \
    https://repo1.maven.org/maven2/org/rdfhdt/hdt-java-package/${HDT_JAVA_PACKAGE_VERSION}/hdt-java-package-${HDT_JAVA_PACKAGE_VERSION}-distribution.tar.gz \
  && tar -xzf /tmp/hdt-java-package.tar.gz -C /opt/hdt-java --strip-components=1 \
  && rm -f /tmp/hdt-java-package.tar.gz

COPY --from=build-hdt-cpp /usr/local/bin/rdf2hdt /usr/local/bin/rdf2hdt
COPY --from=build-hdt-cpp /usr/local/bin/hdt2rdf /usr/local/bin/hdt2rdf
COPY --from=build-hdt-cpp /usr/local/lib/libcds* /usr/local/lib/
COPY --from=build-hdt-cpp /usr/local/lib/libhdt* /usr/local/lib/
COPY --from=build-hdt-cpp /opt/third_party_licenses/ /usr/share/licenses/vcf-rdfizer/
COPY THIRD_PARTY_NOTICES.md /usr/share/licenses/vcf-rdfizer/THIRD_PARTY_NOTICES.md
COPY src/*.sh /opt/vcf-rdfizer/
COPY src/*.py /opt/vcf-rdfizer/

RUN chmod +x /opt/vcf-rdfizer/*.sh \
  && find /opt/hdt-java/bin -type f -exec chmod +x {} \; \
  && chmod +x /usr/local/bin/rdf2hdt \
  && chmod +x /usr/local/bin/hdt2rdf

ENV RMLSTREAMER_JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar
ENV JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar
ENV HDT_JAVA_HOME=/opt/hdt-java
ENV RDF2HDT_BIN=/usr/local/bin/rdf2hdt
ENV HDT2RDF_BIN=/usr/local/bin/hdt2rdf
ENV COTTAS_PYTHON_BIN=/opt/pycottas-venv/bin/python
ENV LD_LIBRARY_PATH=/usr/local/lib

# COTTAS creates a temporary DuckDB database in the container working
# directory. The wrapper runs containers as the host UID/GID, so this path
# must be writable without requiring root or creating root-owned host files.
RUN mkdir -p /work && chmod 1777 /work
WORKDIR /work
