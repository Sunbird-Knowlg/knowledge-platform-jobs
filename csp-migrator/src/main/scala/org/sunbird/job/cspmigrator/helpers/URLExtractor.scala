package org.sunbird.job.cspmigrator.helpers

import java.util
import java.util.regex.Pattern
import scala.collection.JavaConverters._

trait URLExtractor {

  // Function to extract all the URL// Function to extract all the URL from the string
  def extarctUrls(str: String): List[String] = {
    // Creating an empty ArrayList
    val list = new util.ArrayList[String]

    // Regular Expression to extract URL from the string
    val regex = "\\b((?:https?):" + "//[-a-zA-Z0-9+&@#/%?=" + "~_|!:, .;]*[-a-zA-Z0-9+" + "&@#/%=~_|])" //(?<=")https?:\/\/[^\"]+
    // Compile the Regular Expression
    val p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE)
    // Find the match between string and the regular expression
    val m = p.matcher(str)
    // Find the next subsequence of the input subsequence that find the pattern
    while (m.find) {
      // Find the substring from the first index of match result to the last index of match result and add in the list
      list.add(str.substring(m.start(0), m.end(0)))
    }
    // IF there no URL present
    if (list.size == 0) {
      System.out.println("-1")
      return List.empty[String]
    }
    // Print all the URLs stored

    list.asScala.toList
  }
}