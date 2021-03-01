package org.sunbird.publish.helpers

import java.io.StringWriter
import java.util.Properties

import org.apache.velocity.VelocityContext
import org.apache.velocity.app.Velocity

trait ObjectTemplateGenerator {


    def handleHtmlTemplate(templateName: String, context: Map[String,AnyRef]): String = {
        initVelocityEngine(templateName)
        val veContext: VelocityContext = new VelocityContext()
        context.foreach(entry => veContext.put(entry._1, entry._2))
        val writer:StringWriter = new StringWriter()
        Velocity.mergeTemplate(templateName, "UTF-8", veContext, writer)
        writer.toString
    }

    private def initVelocityEngine(templateName: String): Unit = {
        val properties = new Properties()
        if (!templateName.startsWith("http") && !templateName.startsWith("/")) {
            properties.setProperty("resource.loader", "class")
            properties.setProperty("class.resource.loader.class", "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader")
        }
        Velocity.init(properties)
    }
}
