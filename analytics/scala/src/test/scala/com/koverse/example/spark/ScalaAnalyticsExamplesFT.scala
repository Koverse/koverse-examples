package com.koverse.example.spark

import com.koverse.com.google.common.collect.Lists.newArrayList
import com.koverse.thrift.{TConfigValue, TConfigValueType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import java.util
import java.util.UUID

@RunWith(classOf[JUnitRunner])
class ScalaAnalyticsExamplesFT extends FunSuite with TestUtils {


  test("Word Counter DataFrame test") { // automation set up - see automation example for details
    val pages = "baseball soccer"
    var importDataSet = createDataSet(UUID.randomUUID.toString)
    val tranformDataSet = createDataSet(importDataSet.getName + "-copy")
    importDataSet = setUpImport(importDataSet, pages)

    // add in tranform
    val transformType = "wordCountDataFrameExample"
    val transformOptions = new util.HashMap[String, TConfigValue]
    val textFieldValue = new TConfigValue
    textFieldValue.setType(TConfigValueType.STRING)
    textFieldValue.setStringValue("article")
    transformOptions.put("textFieldName", textFieldValue)

    // configure the transform to read from the Wikipedia articles data set
    val inputCollectionValue = new TConfigValue
    inputCollectionValue.setType(TConfigValueType.STRING_LIST)
    inputCollectionValue.setStringList(newArrayList(importDataSet.getId))
    transformOptions.put("inputDataset", inputCollectionValue)

    // configure the transform to write results to the word count data set
    val outputCollectionValue = new TConfigValue
    outputCollectionValue.setType(TConfigValueType.STRING)
    outputCollectionValue.setStringValue(tranformDataSet.getId)
    transformOptions.put("outputCollection", outputCollectionValue)
    configureAndSaveTransform(transformType, transformOptions)

    // start import and transform processes - this can take several minutes
    executeAndMonitorImportFlow(importDataSet.getId, tranformDataSet.getId)

    // show me the the contents of the transform data set
    val results = client.getAllRecords(tranformDataSet.getName, 10)

    import scala.collection.JavaConversions._
    println("First 10 word counts for " + pages)
    for (result <- results) {
      println(s"${result.get("lowerWord")} ${result.get("count")}")
    }
    /* comment this out if you want to see the data sets in the UI */
    // tidy up afterward
    tearDownDataFlow(importDataSet.getName());
  }

  test("Word Counter DataSet test") { // automation set up - see automation example for details
    val pages = "baseball soccer"
    var importDataSet = createDataSet(UUID.randomUUID.toString)
    val tranformDataSet = createDataSet(importDataSet.getName + "-dataset-wordcount")
    importDataSet = setUpImport(importDataSet, pages)

    // add in tranform
    val transformType = "wordCountDatasetExample"
    val transformOptions = new util.HashMap[String, TConfigValue]
    val textFieldValue = new TConfigValue
    textFieldValue.setType(TConfigValueType.STRING)
    textFieldValue.setStringValue("article")
    transformOptions.put("textFieldName", textFieldValue)

    // configure the transform to read from the Wikipedia articles data set
    val inputCollectionValue = new TConfigValue
    inputCollectionValue.setType(TConfigValueType.STRING_LIST)
    inputCollectionValue.setStringList(newArrayList(importDataSet.getId))
    transformOptions.put("messageDataset", inputCollectionValue)

    // configure the transform to write results to the word count data set
    val outputCollectionValue = new TConfigValue
    outputCollectionValue.setType(TConfigValueType.STRING)
    outputCollectionValue.setStringValue(tranformDataSet.getId)
    transformOptions.put("outputCollection", outputCollectionValue)
    configureAndSaveTransform(transformType, transformOptions)

    // start import and transform processes - this can take several minutes
    executeAndMonitorImportFlow(importDataSet.getId, tranformDataSet.getId)

    // show me the the contents of the transform data set
    val results = client.getAllRecords(tranformDataSet.getName, 10)

    import scala.collection.JavaConversions._
    println("First 10 word counts for " + pages )
    for (result <- results) {
      println(s"${result.get("lowerWord")} ${result.get("count")}")
    }
    /* comment this out if you want to see the data sets in the UI */
    // tidy up afterward
//    tearDownDataFlow(importDataSet.getName());
  }

}
