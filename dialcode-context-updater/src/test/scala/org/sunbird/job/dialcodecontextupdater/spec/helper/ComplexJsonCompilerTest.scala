package org.sunbird.job.dialcodecontextupdater.spec.helper

import org.junit.Test
import org.sunbird.job.util.ComplexJsonCompiler


class ComplexJsonCompilerTest {
  val contextMapFile: String = "https://sunbirddev.blob.core.windows.net/sunbird-dial-dev/schemas/local/dialcode/contextMapping.json"
  @Test
  @throws[Exception]
  def testTBUnit(): Unit = {
    val contextType = "textbook_unit"
    val compiledContext = ComplexJsonCompiler.createConsolidatedSchema(contextMapFile, contextType)
    println(compiledContext)
    assert(!compiledContext.contains("$ref"))
  }

  @Test
  @throws[Exception]
  def testTextBook(): Unit = {
    val contextType = "digital_textbook"
    val compiledContext = ComplexJsonCompiler.createConsolidatedSchema(contextMapFile, contextType)
    assert(!compiledContext.contains("$ref"))
  }

  @Test
  @throws[Exception]
  def testCourseUnit(): Unit = {
    val contextType = "course_unit"
    val compiledContext = ComplexJsonCompiler.createConsolidatedSchema(contextMapFile, contextType)
    assert(!compiledContext.contains("$ref"))
  }

  @Test
  @throws[Exception]
  def testCourse(): Unit = {
    val contextType = "course"
    val compiledContext = ComplexJsonCompiler.createConsolidatedSchema(contextMapFile, contextType)
    assert(!compiledContext.contains("$ref"))
  }

}