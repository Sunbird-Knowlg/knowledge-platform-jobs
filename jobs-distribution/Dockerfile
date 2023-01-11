FROM sunbird/flink:1.13.5-scala_2.12-java11

USER root
RUN apt-get update
RUN apt-get install -y imagemagick

COPY target/jobs-distribution-1.0.tar.gz /tmp
USER flink
RUN tar -xvf /tmp/jobs-distribution-1.0.tar.gz -C $FLINK_HOME/lib/
USER root
RUN rm -f /tmp/jobs-distribution-1.0.tar.gz
RUN rm -f $FLINK_HOME/lib/flink-s3-fs-hadoop-1.13.5.jar 
USER flink
