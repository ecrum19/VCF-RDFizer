FROM maven:3.9-eclipse-temurin-11 AS build-hdt

RUN apt-get update \
  && apt-get install -y --no-install-recommends git \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
RUN git clone https://github.com/rdfhdt/hdt-java.git

WORKDIR /opt/hdt-java
RUN mvn -q clean install -DskipTests

WORKDIR /opt/hdt-java/hdt-java-cli
RUN mvn -q clean install -DskipTests

RUN mkdir -p /opt/hdt \
  && cp /opt/hdt-java/hdt-java/target/hdt-java*.jar /opt/hdt/ \
  && cp /opt/hdt-java/hdt-java-cli/target/hdt-java-cli*.jar /opt/hdt/


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
    nodejs \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/rmlstreamer \
  && curl -fsSL \
    -o /opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar \
    https://github.com/RMLio/RMLStreamer/releases/download/v${RMLSTREAMER_VERSION}/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar

ENV RMLSTREAMER_JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar
ENV JAR=/opt/rmlstreamer/RMLStreamer-v${RMLSTREAMER_VERSION}-standalone.jar

COPY --from=build-hdt /opt/hdt /opt/hdt
COPY src/*.sh /opt/vcf-rdfizer/

RUN chmod +x /opt/vcf-rdfizer/*.sh

WORKDIR /work
