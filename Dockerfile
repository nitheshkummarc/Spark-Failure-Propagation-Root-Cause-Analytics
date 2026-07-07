# =============================================================================
# Spark Root-Cause Analytics — Dockerfile
# Development environment for reproducing the Spark RCA pipeline.
#
# Multi-stage build:
#   Stage 1: Build the assembly JAR using SBT
#   Stage 2: Spark runtime with the built JAR
# =============================================================================

# --- Stage 1: Build ---
FROM eclipse-temurin:11-jdk AS builder

RUN apt-get update && apt-get install -y curl bash && \
    curl -fL "https://github.com/sbt/sbt/releases/download/v1.9.7/sbt-1.9.7.tgz" | tar xz -C /usr/local && \
    ln -s /usr/local/sbt/bin/sbt /usr/local/bin/sbt && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY build.sbt .
COPY project/build.properties project/plugins.sbt project/
RUN sbt update

COPY src/ src/
RUN sbt assembly

# --- Stage 2: Runtime ---
FROM apache/spark:3.5.1

USER root
COPY --from=builder /build/target/scala-2.12/spark-rca-assembly.jar /opt/spark/jars/

# Copy pipeline scripts
COPY scripts/ /opt/spark-rca/scripts/

WORKDIR /opt/spark-rca
USER spark
