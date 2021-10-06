package org.sunbird.spec

import org.scalatest.{FlatSpec, Matchers}
import org.sunbird.job.util.FileUtils

class FileUtilsSpec extends FlatSpec with Matchers {

	"getBasePath with empty identifier" should "return the path" in {
		val result = FileUtils.getBasePath("")
		result.nonEmpty shouldBe(true)
	}
}
