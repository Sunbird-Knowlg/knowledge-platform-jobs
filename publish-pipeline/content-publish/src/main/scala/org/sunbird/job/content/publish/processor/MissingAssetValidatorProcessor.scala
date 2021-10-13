package org.sunbird.job.content.publish.processor

import org.sunbird.job.exception.InvalidInputException

import java.io.File

trait MissingAssetValidatorProcessor extends IProcessor {

    abstract override def process(ecrf: Plugin): Plugin = {
        validateMissingAssets(ecrf)
        super.process(ecrf)
    }

    def getMediaId(media: Media): String = {
        if(null != media.data && media.data.nonEmpty){
            val plugin = media.data.get("plugin")
            val ver = media.data.get("version")
            if((null != plugin && plugin.toString.nonEmpty) && (null != ver && ver.toString.nonEmpty))
                media.id + "_" + plugin+ "_" + ver
            else media.id
        }else media.id
    }

    def validateMissingAssets(ecrf: Plugin): Any = {
        if(null != ecrf.manifest){
            val medias:List[Media] = ecrf.manifest.medias
            if(null != medias){
                val mediaIds = medias.map(media => getMediaId(media)).toList
                if(mediaIds.size != mediaIds.distinct.size)
                    throw new InvalidInputException("Error! Duplicate Asset Id used in the manifest. Asset Ids are: "
                            + mediaIds.groupBy(identity).mapValues(_.size).filter(p => p._2 > 1).keySet)

                val nonYoutubeMedias = medias.filter(media => !"youtube".equalsIgnoreCase(media.`type`))
                nonYoutubeMedias.map(media => {
                    if(widgetTypeAssets.contains(media.`type`) && !new File(getBasePath() + File.separator + "widgets" + File.separator + media.src).exists())
                        throw new InvalidInputException("Error! Missing Asset.  | [Asset Id '" + media.id)
                    else if(!widgetTypeAssets.contains(media.`type`) && !media.src.startsWith("http") && !media.src.startsWith("https") && !new File(getBasePath() + File.separator + "assets" + File.separator + media.src).exists())
                        throw new InvalidInputException("Error! Missing Asset.  | [Asset Id '" + media.id)
                    else if (!widgetTypeAssets.contains(media.`type`) && (media.src.startsWith("http") || media.src.startsWith("https")) && !new File(getBasePath() + File.separator + "assets" + File.separator + media.src.split("/").last).exists())
                      throw new InvalidInputException("Error! Missing Asset.  | [Asset Id '" + media.id)
                })
            }
        }
    }
}
