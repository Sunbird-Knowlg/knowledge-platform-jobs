package org.sunbird.job.util

;

class OntologyEngineContext {
    private val kfClient = new KafkaClientUtil

    def kafkaClient = kfClient
}
