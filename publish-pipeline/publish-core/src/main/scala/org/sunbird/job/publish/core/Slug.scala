package org.sunbird.job.publish.core

import net.sf.junidecode.Junidecode
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils

import java.io.File
import java.net.URLDecoder
import java.text.Normalizer
import java.text.Normalizer.Form
import java.util.Locale

object Slug {

  private val NONLATIN: String = "[^\\w-\\.]"
  private val WHITESPACE: String = "[\\s]"
  private val DUPDASH: String = "-+"

  def createSlugFile(file: File): File = {
    try {
      val name = file.getName
      val slug = Slug.makeSlug(name, isTransliterate = true)
      if (!StringUtils.equals(name, slug)) {
        val newName = FilenameUtils.getFullPath(file.getAbsolutePath) + File.separator + slug
        new File(newName)
      } else file
    } catch {
      case e: Exception =>
        e.printStackTrace()
        file
    }
  }

  def makeSlug(input: String): String = {
    makeSlug(input, isTransliterate = false)
  }

  def makeSlug(input: String, isTransliterate: Boolean): String = {
    // Validate the input
    if (input == null) throw new IllegalArgumentException("Input is null")
    // Remove extra spaces
    val trimmed = input.trim
    // Remove URL encoding
    val urlEncoded = urlDecode(trimmed)
    // If transliterate is required
    // Transliterate & cleanup
    val transliterated = if (isTransliterate) {
      transliterate(urlEncoded)
      //transliterated = removeDuplicateChars(transliterated);
    } else urlEncoded
    // Replace all whitespace with dashes
    val nonWhitespaced = transliterated.replaceAll(WHITESPACE, "-")
    // Remove all accent chars
    val normalized = Normalizer.normalize(nonWhitespaced, Form.NFD)
    // Remove all non-latin special characters
    val nonLatin = normalized.replaceAll(NONLATIN, "")
    // Remove any consecutive dashes
    val normalizedDashes = normalizeDashes(nonLatin)
    // Validate before returning
    validateResult(normalizedDashes, input)
    // Slug is always lowercase
    normalizedDashes.toLowerCase(Locale.ENGLISH)
  }

  private def validateResult(input: String, origInput: String): Unit = {
    if (input.isEmpty) throw new IllegalArgumentException("Failed to cleanup the input " + origInput)
  }

  def transliterate(input: String): String = Junidecode.unidecode(input)

  def urlDecode(input: String): String = {
    try
      URLDecoder.decode(input, "UTF-8")
    catch {
      case ex: Exception => input
    }
  }

  def removeDuplicateChars(text: String): String = {
    val ret = new StringBuilder(text.length)
    if (text.isEmpty) "" else {
      // Zip with Index returns a tuple (character, index)
      ret.append(text.charAt(0))
      text.toCharArray.zipWithIndex
        .foreach(zippedChar => {
          if (zippedChar._2 != 0 && zippedChar._1 != text.charAt(zippedChar._2 - 1))
            ret.append(zippedChar._1)
        })
      ret.toString()
    }
  }

  def normalizeDashes(text: String): String = {
    val clean = text.replaceAll(DUPDASH, "-")
    if (clean == "-" || clean == "--") ""
    else {
      val startIdx = if (clean.startsWith("-")) 1 else 0
      val endIdx = if (clean.endsWith("-")) 1 else 0
      clean.substring(startIdx, clean.length - endIdx)
    }
  }

}