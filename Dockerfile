FROM eclipse-temurin:11-jre AS build-hdt-cpp

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
RUN git clone --depth 1 https://github.com/rdfhdt/hdt-cpp.git

WORKDIR /opt/hdt-cpp
RUN ./autogen.sh \
  && ./configure \
  && make -j"$(nproc)" \
  && make install


FROM eclipse-temurin:11-jre

ARG RMLSTREAMER_VERSION=2.5.0

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
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/rmlstreamer \
  && curl -fsSL \
    -o /opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar \
    https://github.com/RMLio/RMLStreamer/releases/download/v${RMLSTREAMER_VERSION}/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar

COPY --from=build-hdt-cpp /usr/local/bin/rdf2hdt /usr/local/bin/rdf2hdt
COPY --from=build-hdt-cpp /usr/local/bin/hdt2rdf /usr/local/bin/hdt2rdf
COPY --from=build-hdt-cpp /usr/local/lib/libcds* /usr/local/lib/
COPY --from=build-hdt-cpp /usr/local/lib/libhdt* /usr/local/lib/
COPY src/*.sh /opt/vcf-rdfizer/

RUN chmod +x /opt/vcf-rdfizer/*.sh \
  && chmod +x /usr/local/bin/rdf2hdt \
  && chmod +x /usr/local/bin/hdt2rdf

ENV RMLSTREAMER_JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar
ENV JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar
ENV RDF2HDT_BIN=/usr/local/bin/rdf2hdt
ENV HDT2RDF_BIN=/usr/local/bin/hdt2rdf
ENV LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /work
