FROM sunbird/flink:1.13.5-scala_2.12-java11

USER root
RUN apt-get update
RUN apt-get install -y imagemagick

COPY target/jobs-distribution-1.0.tar.gz /tmp
USER flink
RUN tar -xvf /tmp/jobs-distribution-1.0.tar.gz -C $FLINK_HOME/lib/
RUN mkdir $FLINK_HOME/plugins/s3-fs-presto
RUN mkdir $FLINK_HOME/plugins/s3-fs-hadoop
RUN cp $FLINK_HOME/opt/flink-s3-fs-presto-1.13.5.jar $FLINK_HOME/plugins/s3-fs-presto/
RUN cp $FLINK_HOME/opt/flink-s3-fs-hadoop-1.13.5.jar $FLINK_HOME/plugins/s3-fs-hadoop/
USER root
RUN rm -f /tmp/jobs-distribution-1.0.tar.gz
USER flink
