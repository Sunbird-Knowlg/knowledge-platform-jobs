package org.sunbird.job.dialcodecontextupdater.helpers

import org.slf4j.LoggerFactory
import org.sunbird.job.dialcodecontextupdater.domain.Event
import org.sunbird.job.dialcodecontextupdater.task.DialcodeContextUpdaterConfig
import org.sunbird.job.dialcodecontextupdater.util.DialcodeContextUpdaterConstants
import org.sunbird.job.exception.ServerException
import org.sunbird.job.util._

import java.util


trait DialcodeContextUpdater {

	private[this] val logger = LoggerFactory.getLogger(classOf[DialcodeContextUpdater])

	def updateContext(config: DialcodeContextUpdaterConfig, event: Event, httpUtil: HttpUtil): Map[String, AnyRef] = {
		logger.info("DialcodeContextUpdater::updateContext:: Processing event for dial code context update operation for event object : " + event.obj)
		logger.info("DialcodeContextUpdater::updateContext:: event edata : " + event.eData)
		val identifier = event.identifier
		val dialcode = event.dialcode
		val channel = event.channel
		if(identifier.nonEmpty && event.action.equalsIgnoreCase("dialcode-context-update")) {
			// 1. Read primaryCategory of the content identifier using Search API
			// 2. Fetch contextMap for the content primaryCategory
			// 3. Filter for search fields from the contextMap
			// 4. Call Search API with dialcode and mode=collection
			// 5. Fetch unit and rootNode output from the response
			// 6. compose contextInfo and invoke updateDIALcode v4 API
			logger.info("DialcodeContextUpdater::updateContext:: Context Update Starting for dialcode: " + dialcode + " || identifier: " + identifier + " || channel: " + channel)
			val identifierObj = try {
				searchContent("", identifier, config.identifierSearchFields, config, httpUtil)
			} catch {
				case se: ServerException => if(se.getMessage.contains("No content linking was found for dialcode")) {
					try {
						Thread.sleep(config.nodeESSyncWaitTime)
						searchContent("", identifier, config.identifierSearchFields, config, httpUtil)
					}
					catch {
						case ie: InterruptedException =>	throw new ServerException("ERR_DIAL_CONTENT_NOT_FOUND", "No content linking was found for dialcode: " + dialcode + " - to identifier: " + identifier)
						case ex: Exception => throw ex
					}
				} else throw se
				case ex: Exception => throw ex
			}

			logger.info("DialcodeContextUpdater:: updateContext:: config.contextMapFilePath: " + config.contextMapFilePath)

			if(config.contextMapFilePath.isEmpty) {
				throw new ServerException("ERR_CONTEXT_MAP_NOT_FOUND", "Context mapping file path was not found for dialcode: " + dialcode + " - to identifier: " + identifier)
			}

			val primaryCategory = identifierObj.getOrElse("primaryCategory","").asInstanceOf[String].toLowerCase.replaceAll(" ","_")
			val contextMap: Map[String, AnyRef] = getContextMapFields(primaryCategory, config.contextMapFilePath)
			val contextSearchFields: List[String] = fetchFieldsFromMap(contextMap).distinct.filter(rec => !rec.startsWith("@")) ++ List("origin", "originData")
			logger.info("DialcodeContextUpdater:: updateContext:: searchFields: " + contextSearchFields)

			val contextInfoSearchData: Map[String, AnyRef] =	try {
				searchContent(dialcode, identifier, contextSearchFields, config, httpUtil)
			} catch {
				case se: ServerException => if(se.getMessage.contains("No content linking was found for dialcode")) {
					try {
						Thread.sleep(config.nodeESSyncWaitTime)
						searchContent(dialcode, identifier, contextSearchFields, config, httpUtil)
					}
					catch {
						case ie: InterruptedException =>	throw new ServerException("ERR_DIAL_CONTENT_NOT_FOUND", "No content linking was found for dialcode: " + dialcode + " - to identifier: " + identifier)
						case ex: Exception => throw ex
					}
				} else throw se
				case ex: Exception => throw ex
			}
			logger.info("DialcodeContextUpdater:: updateContext:: contextInfoSearchData: " + ScalaJsonUtil.serialize(contextInfoSearchData))

			// Filter for necessary fields
			val filteredParentInfo = if(contextInfoSearchData.contains("parentInfo")) contextInfoSearchData.getOrElse("parentInfo", Map.empty[String, AnyRef]).asInstanceOf[Map[String, AnyRef]].filter(rec => contextSearchFields.contains(rec._1)) else Map.empty[String, AnyRef]
			val inputContextData = contextInfoSearchData.filter(rec => contextSearchFields.contains(rec._1))

			val finalFilteredData = if(filteredParentInfo!=null && filteredParentInfo.nonEmpty) inputContextData + ("parentInfo" -> filteredParentInfo) else inputContextData
			logger.info("DialcodeContextUpdater:: updateContext:: finalFilteredData: " + finalFilteredData)

			val contextDataToUpdate = contextMap.map(rec => {
				rec._2 match {
					case stringVal: String => if(!rec._1.equalsIgnoreCase("@type") && finalFilteredData.contains(stringVal)) (rec._1 -> finalFilteredData(stringVal)) else (rec._1 -> rec._2)
					case objectVal: Map[String, AnyRef] =>  (rec._1 -> objectVal.map(record => {
						record._2 match {
							case stringValSubLevel: String =>
								if (record._1.equalsIgnoreCase("@type")) (record._1 -> stringValSubLevel)
								else if (!record._1.equalsIgnoreCase("@type") && finalFilteredData.contains(rec._1) && finalFilteredData(rec._1).isInstanceOf[String]) if(finalFilteredData.contains(stringValSubLevel))(record._1 -> finalFilteredData(stringValSubLevel)) else (record._1 -> null)
								else if (!record._1.equalsIgnoreCase("@type") && finalFilteredData.contains(rec._1) && finalFilteredData(rec._1).asInstanceOf[Map[String, AnyRef]].contains(stringValSubLevel)) (record._1 -> finalFilteredData(rec._1).asInstanceOf[Map[String, AnyRef]](stringValSubLevel))
								else (record._1 -> null)
							case objectValSubLevel: Map[String, AnyRef] =>  (record._1 -> objectValSubLevel.map(l2Record => {
								if(finalFilteredData.contains(rec._1) && finalFilteredData(rec._1).asInstanceOf[Map[String, AnyRef]].contains(l2Record._2.asInstanceOf[String]))
									(l2Record._1 -> finalFilteredData(rec._1).asInstanceOf[Map[String, AnyRef]](l2Record._2.asInstanceOf[String])) else (l2Record._1 -> null)
							}).filter(checkRec => checkRec._2!=null))
						}
					}).filter(filterRec=>filterRec._2!=null))
				}
			})

			logger.info("DialcodeContextUpdater:: updateContext:: serialize - contentDataToUpsert: " + ScalaJsonUtil.serialize(contextDataToUpdate))

			updateDIALContext(config, dialcode, channel, ScalaJsonUtil.serialize(contextDataToUpdate))(httpUtil)
		} else {
			// check if contextInfo is available in metadata. If Yes, update to null
			val dialCodeInfo = readDialCode(config, dialcode, channel)(httpUtil)
			if(dialCodeInfo.contains("contextInfo") && dialCodeInfo("contextInfo") != null)
				updateDIALContext(config, dialcode, channel, null)(httpUtil)
		}

		readDialCode(config, dialcode, channel)(httpUtil)
	}

	def readDialCode(config: DialcodeContextUpdaterConfig, dialcode: String, channel: String) (implicit httpUtil: HttpUtil): Map[String, AnyRef] = {
		val dialCodeContextReadUrl = config.dialServiceBaseUrl + config.dialcodeContextReadPath + dialcode
		val headers = Map[String, String]("X-Channel-Id" -> channel, "Content-Type"->"application/json")
		val dialcodeResponse: HTTPResponse = httpUtil.get(dialCodeContextReadUrl, headers)
		val obj = JSONUtil.deserialize[Map[String, AnyRef]](dialcodeResponse.body)
		obj("result").asInstanceOf[Map[String, AnyRef]]("dialcode").asInstanceOf[Map[String, AnyRef]]
	}

	def getContextMapFields(contextType: String, contextMapFileURL: String): Map[String, AnyRef] = {
		try {
			val contextMap: Map[String, AnyRef] = ScalaJsonUtil.deserialize[Map[String, AnyRef]](ComplexJsonCompiler.createConsolidatedSchema(contextMapFileURL, contextType))
			logger.info("DialcodeContextUpdater:: getContextSearchFields:: contextMap: " + contextMap)
			contextMap
		} catch {
			case e: Exception =>
				throw new ServerException("ERR_CONTEXT_MAP_READING","Error in reading context map file: " + e.getMessage)
		}
	}

	def fetchFieldsFromMap(inputMap: Map[String, AnyRef]): List[String] = {
		inputMap.flatMap(rec => {
			rec._2 match {
				case strVal: String => List(strVal)
				case objVal: Map[String, AnyRef] => fetchFieldsFromMap(objVal)
			}
		}).toList
	}


	private def searchContent(dialcode: String, identifier: String, searchFields: List[String], config: DialcodeContextUpdaterConfig, httpUtil: HttpUtil): Map[String, AnyRef] = {
		val reqMap = new java.util.HashMap[String, AnyRef]() {
			put(DialcodeContextUpdaterConstants.REQUEST, new java.util.HashMap[String, AnyRef]() {
				put(DialcodeContextUpdaterConstants.FILTERS, new java.util.HashMap[String, AnyRef]() {
					put(DialcodeContextUpdaterConstants.VISIBILITY, new util.ArrayList[String]() {
						add("Default")
						add("Parent")
					})
					put(DialcodeContextUpdaterConstants.IDENTIFIER, identifier)
					if(dialcode.nonEmpty) {
						put(DialcodeContextUpdaterConstants.DIALCODES, new util.ArrayList[String](){
							add(dialcode)
						})
					}
				})
				put(DialcodeContextUpdaterConstants.FIELDS, searchFields.toArray[String])
				if(dialcode.nonEmpty) {
					put(DialcodeContextUpdaterConstants.SEARCH_MODE, config.searchMode)
				}
			})
		}

		val requestUrl = s"${config.searchServiceBaseUrl}/v3/search"
		logger.info("DialcodeContextUpdater :: searchContent :: Search Content requestUrl: " + requestUrl)
		logger.info("DialcodeContextUpdater :: searchContent :: Search Content reqMap: " + reqMap)
		val httpResponse = httpUtil.post(requestUrl, JSONUtil.serialize(reqMap))
		if (httpResponse.status == 200) {
			val response = JSONUtil.deserialize[Map[String, AnyRef]](httpResponse.body)
			val result = response.getOrElse("result", Map[String, AnyRef]()).asInstanceOf[Map[String, AnyRef]]
			val content = result.getOrElse("content", List[Map[String,AnyRef]]()).asInstanceOf[List[Map[String,AnyRef]]]
			val collections = result.getOrElse("collections", List[Map[String,AnyRef]]()).asInstanceOf[List[Map[String,AnyRef]]]
			val count = result.getOrElse("count", 0).asInstanceOf[Int]
			if(count>0) {
				if(collections.isEmpty) {
					content.head
				} else {
					content.head + ("parentInfo" -> collections.filter(record => !record.contains("origin")).head)
				}
			} else {
				throw new ServerException("ERR_DIAL_CONTENT_NOT_FOUND", "No content linking was found for dialcode: " + dialcode + " - to identifier: " + identifier)
			}
		} else {
			throw new ServerException("ERR_SEARCH_API_CALL", "Invalid Response received while searching content for dialcode: " + dialcode)
		}
	}

	def updateDIALContext(config: DialcodeContextUpdaterConfig, dialcode: String, channel: String, contextInfo: String) (implicit httpUtil: HttpUtil): Boolean = {
		val dialCodeContextUpdateUrl = config.dialServiceBaseUrl + config.dialcodeContextUpdatePath + dialcode
		val requestBody = if(contextInfo!=null) "{\"request\": {\"dialcode\": {\"contextInfo\":" + contextInfo + "}}}"
		else "{\"request\": {\"dialcode\": {\"contextInfo\": null }}}"
		logger.info("DialcodeContextUpdater :: updateDIALContext :: Update context requestBody: " + requestBody)
		val headers = Map[String, String]("X-Channel-Id" -> channel, "Content-Type"->"application/json")
		val response:HTTPResponse = httpUtil.patch(dialCodeContextUpdateUrl, requestBody, headers)

		if(response.status == 200){
			true
		} else {
			logger.error("Error while updating context for dialcode: " + dialcode + " :: "+response.body)
			throw new ServerException("ERR_DIAL_CONTEXT_UPDATE_API","Error while updating context for dialcode: " + dialcode + " :: "+response.body)
		}
	}

}
