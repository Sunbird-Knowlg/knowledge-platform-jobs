FROM flink:1.13.6-scala_2.12-java11
COPY search-indexer/target/search-indexer-1.0.0.jar /opt/flink/lib
COPY transaction-event-processor/target/transaction-event-processor-1.0.0.jar /opt/flink/lib
COPY asset-enrichment/target/asset-enrichment-1.0.0.jar /opt/flink/lib
COPY post-publish-processor/target/post-publish-processor-1.0.0.jar /opt/flink/lib
COPY dialcode-context-updater/target/dialcode-context-updater-1.0.0.jar /opt/flink/lib
COPY qrcode-image-generator/target/qrcode-image-generator-1.0.0.jar /opt/flink/lib
COPY video-stream-generator/target/video-stream-generator-1.0.0.jar /opt/flink/lib
COPY publish-pipeline/content-publish/target/content-publish-1.0.0.jar /opt/flink/lib
COPY jobs-core/target/jobs-core-1.0.0.jar /opt/flink/lib

USER flink