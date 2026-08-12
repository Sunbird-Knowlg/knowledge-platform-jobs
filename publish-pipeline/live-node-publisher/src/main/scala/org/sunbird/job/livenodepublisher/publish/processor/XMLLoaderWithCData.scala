package org.sunbird.job.livenodepublisher.publish.processor

import org.xml.sax.InputSource
import org.xml.sax.ext.{DefaultHandler2, LexicalHandler}

import scala.xml.factory.XMLLoader
import scala.xml.parsing.FactoryAdapter
import scala.xml.{Elem, PCData, SAXParser, TopScope}

object XMLLoaderWithCData extends XMLLoader[Elem] {
	def lexicalHandler(adapter: FactoryAdapter): LexicalHandler =
		new DefaultHandler2 {
			def captureCData(): Unit = {
				adapter.hStack = PCData(adapter.buffer.toString) :: adapter.hStack
				adapter.buffer.clear()
			}

			override def startCDATA(): Unit = adapter.captureText()
			override def endCDATA(): Unit = captureCData()
		}

	override def loadXML(source: InputSource, parser: SAXParser): Elem = {
		val newAdapter = adapter

		val xmlReader = parser.getXMLReader
		xmlReader.setProperty(
			"http://xml.org/sax/properties/lexical-handler",
			lexicalHandler(newAdapter))

		newAdapter.scopeStack = TopScope :: newAdapter.scopeStack
		parser.parse(source, newAdapter)
		newAdapter.scopeStack = newAdapter.scopeStack.tail

		newAdapter.rootElem.asInstanceOf[Elem]
	}
}
