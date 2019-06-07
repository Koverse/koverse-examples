package com.koverse.examples.analytics;

import static com.koverse.com.google.common.collect.Lists.newArrayList;

import com.koverse.sdk.data.SimpleRecord;
import com.koverse.thrift.TConfigValue;
import com.koverse.thrift.TConfigValueType;
import com.koverse.thrift.collection.TCollection;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.thrift.TException;
import org.junit.Test;

public class JavaAnalyticsExamplesFT {

  // This functional test should be run after the transforms are built and uploaded to your Koverse development installation.
  TestUtils utils = new TestUtils();

  @Test
  public void runJavaWordCountTransform() throws IOException, InterruptedException, TException {

    // automation set up - see automation example for details

    TCollection importDataSet = utils.createDataSet(UUID.randomUUID().toString());
    TCollection tranformDataSet = utils.createDataSet(importDataSet.getName() + "-copy");
    importDataSet = utils.setUpImport(importDataSet, "tennis soccer");

    // add in tranform
    String transformType = "spark-word-count";

    Map<String, TConfigValue> transformOptions = new HashMap<>();

    TConfigValue textFieldValue = new TConfigValue();
    textFieldValue.setType(TConfigValueType.STRING);
    textFieldValue.setStringValue("article");
    transformOptions.put("textFieldParam", textFieldValue);

    // configure the transform to read from the Wikipedia articles data set
    TConfigValue inputCollectionValue = new TConfigValue();
    inputCollectionValue.setType(TConfigValueType.STRING_LIST);
    inputCollectionValue.setStringList(newArrayList(importDataSet.getId()));
    transformOptions.put("inputDataset", inputCollectionValue);

    // configure the transform to write results to the word count data set
    TConfigValue outputCollectionValue = new TConfigValue();
    outputCollectionValue.setType(TConfigValueType.STRING);
    outputCollectionValue.setStringValue(tranformDataSet.getId());
    transformOptions.put("outputCollection", outputCollectionValue);
    utils.configureAndSaveTransform(transformType, transformOptions);

    // start import and transform processes - this can take several minutes
    utils.executeAndMonitorImportFlow(importDataSet.getId(), tranformDataSet.getId());

    // show me the the contents of the transform data set
    List<SimpleRecord> results = utils.client.getAllRecords(tranformDataSet.getName(), 10);
    System.out.println(results.size() + " sample records from " + tranformDataSet.getName());
    for (SimpleRecord result : results) {
      System.out.println(result.get("word") + " " + result.get("count"));
    }

    /* comment this out if you want to see the data sets in the UI */
    // tidy up afterward
//    utils.tearDownDataFlow(importDataSet.getName());

  }

  @Test
  public void runSentimentAnalysisTransform() throws TException, InterruptedException{

    // automation set up - see automation example for details

    TCollection importDataSet = utils.createDataSet(UUID.randomUUID().toString());
    TCollection tranformDataSet = utils.createDataSet(importDataSet.getName() + "-sentiment");
    importDataSet = utils.setUpImport(importDataSet, "goodness animosity");

    // add in tranform
    String transformType = "analyze-sentiment";
    Map<String, TConfigValue> transformOptions = new HashMap<>();

    TConfigValue textFieldValue = new TConfigValue();
    textFieldValue.setType(TConfigValueType.STRING);
    textFieldValue.setStringValue("article");
    transformOptions.put("textCol", textFieldValue);

    TConfigValue dateFieldValue = new TConfigValue();
    dateFieldValue.setType(TConfigValueType.STRING);
    dateFieldValue.setStringValue("timestamp");
    transformOptions.put("dateCol", dateFieldValue);

    // configure the transform to read from the Wikipedia articles data set
    TConfigValue inputCollectionValue = new TConfigValue();
    inputCollectionValue.setType(TConfigValueType.STRING_LIST);
    inputCollectionValue.setStringList(newArrayList(importDataSet.getId()));
    transformOptions.put("inputDataset", inputCollectionValue);

    // configure the transform to write results to the word count data set
    TConfigValue outputCollectionValue = new TConfigValue();
    outputCollectionValue.setType(TConfigValueType.STRING);
    outputCollectionValue.setStringValue(tranformDataSet.getId());
    transformOptions.put("outputCollection", outputCollectionValue);

    utils.configureAndSaveTransform(transformType, transformOptions);

    // start import and transform processes - this can take several minutes
    utils.executeAndMonitorImportFlow(importDataSet.getId(), tranformDataSet.getId());

    // show me the the contents of the transform data set
    List<SimpleRecord> results = utils.client.getAllRecords(tranformDataSet.getName(), 10);
    System.out.println(results.size() + " sample records from " + tranformDataSet.getName());
    for (SimpleRecord result : results) {
      System.out.println(result.get("score"));
    }

    /* comment this out if you want to see the data sets in the UI */
    // tidy up afterward
    utils.tearDownDataFlow(importDataSet.getName());

  }

}
