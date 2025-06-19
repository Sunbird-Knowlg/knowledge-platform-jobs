FROM sunbird/flink:1.13.5-scala_2.12-java11

USER root
RUN apt-get update && apt-get dist-upgrade -y && apt-get install -y --no-install-recommends bash curl libcurl4 libdb5.3 libexpat1 libgcrypt20 libldap-2.4-2 libzstd1 zlib1g && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY target/jobs-distribution-1.0.tar.gz /tmp
USER flink
RUN tar -xvf /tmp/jobs-distribution-1.0.tar.gz -C $FLINK_HOME/lib/
RUN mkdir $FLINK_HOME/plugins/s3-fs-presto
RUN cp $FLINK_HOME/opt/flink-s3-fs-presto-1.13.5.jar $FLINK_HOME/plugins/s3-fs-presto/
RUN cp $FLINK_HOME/opt/flink-s3-fs-presto-1.13.5.jar $FLINK_HOME/lib/flink-aaa-s3-fs-presto-1.13.5.jar
USER root
RUN rm -f /tmp/jobs-distribution-1.0.tar.gz
USER flink
