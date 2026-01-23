FROM sunbird/flink:1.13.5-scala_2.12-java11

USER root
RUN apt-get update
RUN apt-get update && apt-get dist-upgrade -y && \
    apt-get install -y --no-install-recommends bash curl libcurl4 libdb5.3 libexpat1 libgcrypt20 libldap-2.4-2 libzstd1 zlib1g imagemagick && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Remove old Guava and add Guava 31.1-jre for jclouds compatibility (SimpleTimeLimiter.create needs 23.0+)
RUN rm -f $FLINK_HOME/lib/guava-*.jar 2>/dev/null || true
ADD https://repo1.maven.org/maven2/com/google/guava/guava/31.1-jre/guava-31.1-jre.jar $FLINK_HOME/lib/
ADD https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar $FLINK_HOME/lib/
RUN chmod 644 $FLINK_HOME/lib/guava-31.1-jre.jar $FLINK_HOME/lib/failureaccess-1.0.1.jar
COPY target/jobs-distribution-1.0.tar.gz /tmp
USER flink
RUN tar -xvf /tmp/jobs-distribution-1.0.tar.gz -C $FLINK_HOME/lib/
RUN mkdir $FLINK_HOME/plugins/s3-fs-presto
RUN cp $FLINK_HOME/opt/flink-s3-fs-presto-1.13.5.jar $FLINK_HOME/plugins/s3-fs-presto/
RUN cp $FLINK_HOME/opt/flink-s3-fs-presto-1.13.5.jar $FLINK_HOME/lib/flink-aaa-s3-fs-presto-1.13.5.jar
USER root
RUN rm -f /tmp/jobs-distribution-1.0.tar.gz

COPY lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar $FLINK_HOME/lib/
COPY lib/gcs-connector-hadoop2-2.2.0.jar $FLINK_HOME/lib/
COPY lib/hadoop-azure-3.3.1.jar /opt/flink/lib/

RUN mkdir -p $FLINK_HOME/plugins/gs-fs-hadoop
RUN mkdir -p $FLINK_HOME/plugins/azure-fs-hadoop

COPY lib/flink-gs-fs-hadoop-1.16.2.jar $FLINK_HOME/plugins/gs-fs-hadoop
COPY lib/flink-azure-fs-hadoop-1.16.2.jar $FLINK_HOME/plugins/azure-fs-hadoop

COPY lib/core-site.xml $FLINK_HOME/conf/
USER flink
