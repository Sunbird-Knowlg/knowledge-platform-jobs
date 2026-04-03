FROM flink:1.18.1-scala_2.12-java11

# CSP selection: pass --build-arg CSP=<azure|aws|gcloud|oci> at image build time (default: azure).
# The Maven build must have been run with the matching profile (-Pazure / -Paws / -Pgcloud / -Poci)
# so that exactly one CSP runtime jar is present in the distribution; Java ServiceLoader picks it up automatically.
# cloud_storage_auth_type defaults to OIDC (Kubernetes Workload Identity).
# Override via K8s ConfigMap or `docker run -e` for local ACCESS_KEY-based development.
ARG CSP=azure
ENV cloud_storage_type=${CSP}
ENV cloud_storage_auth_type=OIDC

USER root
RUN apt-get update && apt-get dist-upgrade -y && \
    apt-get install -y --no-install-recommends bash curl libcurl4 libdb5.3 libexpat1 libgcrypt20 libldap-2.5-0 libzstd1 zlib1g imagemagick && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
COPY target/jobs-distribution-1.0.tar.gz /tmp
USER flink
RUN tar -xvf /tmp/jobs-distribution-1.0.tar.gz -C $FLINK_HOME/lib/
RUN mkdir $FLINK_HOME/plugins/s3-fs-presto
RUN cp $FLINK_HOME/opt/flink-s3-fs-presto-1.18.1.jar $FLINK_HOME/plugins/s3-fs-presto/
USER root
RUN rm -f /tmp/jobs-distribution-1.0.tar.gz

COPY lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar $FLINK_HOME/lib/
COPY lib/gcs-connector-hadoop2-2.2.0.jar $FLINK_HOME/lib/
COPY lib/a-hadoop-azure-3.4.2.jar $FLINK_HOME/lib/
COPY lib/a-hadoop-common-3.4.2.jar $FLINK_HOME/lib/
COPY lib/a-hadoop-auth-3.4.2.jar $FLINK_HOME/lib/

RUN mkdir -p $FLINK_HOME/plugins/gs-fs-hadoop
RUN mkdir -p $FLINK_HOME/plugins/azure-fs-hadoop

RUN cp $FLINK_HOME/opt/flink-gs-fs-hadoop-1.18.1.jar $FLINK_HOME/plugins/gs-fs-hadoop/
RUN cp $FLINK_HOME/opt/flink-azure-fs-hadoop-1.18.1.jar $FLINK_HOME/plugins/azure-fs-hadoop/
# hadoop-azure-3.4.2 must also be in the plugin directory so the plugin's child-first
# classloader finds WorkloadIdentityTokenProvider in its own search path.
# The bundled hadoop-azure inside flink-azure-fs-hadoop is 3.3.x (no WorkloadIdentityTokenProvider),
# and child-first delegation means the parent lib/ jar is not reliably searched.
# We also include hadoop-common and hadoop-auth 3.4.2 to provide new classes
# like org.apache.hadoop.fs.impl.BackReference required by hadoop-azure 3.4.x.
COPY lib/a-hadoop-azure-3.4.2.jar $FLINK_HOME/plugins/azure-fs-hadoop/
COPY lib/a-hadoop-common-3.4.2.jar $FLINK_HOME/plugins/azure-fs-hadoop/
COPY lib/a-hadoop-auth-3.4.2.jar $FLINK_HOME/plugins/azure-fs-hadoop/

COPY lib/core-site.xml $FLINK_HOME/conf/
USER flink
