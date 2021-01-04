package org.sunbird.job.publish.helpers

trait QuestionSetPublish {

	def validateObject(metadata: java.util.Map[String, AnyRef]): Boolean = {
		//TODO: Put all validation logic here
		null != metadata && !metadata.isEmpty
	}

	//TODO: methods for questionset publish
}
