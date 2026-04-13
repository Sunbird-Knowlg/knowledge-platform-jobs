FROM flink:1.18.1-scala_2.12-java11 AS base

USER root
RUN apt-get update && apt-get dist-upgrade -y && \
    apt-get install -y --no-install-recommends bash curl libcurl4 libdb5.3 libexpat1 libgcrypt20 libldap-2.5-0 libzstd1 zlib1g imagemagick && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY target/jobs-distribution-1.0.tar.gz /tmp
USER flink
RUN tar -xvf /tmp/jobs-distribution-1.0.tar.gz -C $FLINK_HOME/lib/
USER root
RUN rm -f /tmp/jobs-distribution-1.0.tar.gz

COPY lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar $FLINK_HOME/lib/
COPY lib/core-site.xml $FLINK_HOME/conf/
ENV cloud_storage_auth_type=OIDC

# ── Azure ──────────────────────────────────────────────────────────────────
FROM base AS azure
ENV cloud_storage_type=azure
RUN mkdir -p $FLINK_HOME/plugins/azure-fs-hadoop && \
    cp $FLINK_HOME/opt/flink-azure-fs-hadoop-1.18.1.jar $FLINK_HOME/plugins/azure-fs-hadoop/
COPY target/azure-plugin/ $FLINK_HOME/plugins/azure-fs-hadoop/
USER flink

# ── GCP ────────────────────────────────────────────────────────────────────
FROM base AS gcloud
ENV cloud_storage_type=gcloud
COPY lib/gcs-connector-hadoop2-2.2.0.jar $FLINK_HOME/lib/
RUN mkdir -p $FLINK_HOME/plugins/gs-fs-hadoop && \
    cp $FLINK_HOME/opt/flink-gs-fs-hadoop-1.18.1.jar $FLINK_HOME/plugins/gs-fs-hadoop/
USER flink

# ── AWS ────────────────────────────────────────────────────────────────────
FROM base AS aws
ENV cloud_storage_type=aws
RUN mkdir -p $FLINK_HOME/plugins/s3-fs-presto && \
    cp $FLINK_HOME/opt/flink-s3-fs-presto-1.18.1.jar $FLINK_HOME/plugins/s3-fs-presto/
USER flink
