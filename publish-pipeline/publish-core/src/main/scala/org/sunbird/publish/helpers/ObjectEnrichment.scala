package org.sunbird.publish.helpers

import org.sunbird.job.util.Neo4JUtil
import org.sunbird.publish.core.ObjectData

trait ObjectEnrichment extends FrameworkDataEnrichment {


  def enrichObject(obj: ObjectData)(implicit neo4JUtil: Neo4JUtil): ObjectData = {
    enrichFrameworkData(obj)
  }



}
