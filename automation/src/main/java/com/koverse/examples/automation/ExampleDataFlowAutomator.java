/*
 * Copyright 2016 Koverse, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.koverse.examples.automation;

import static com.koverse.com.google.common.collect.Lists.newArrayList;

import com.koverse.sdk.data.DataSetResult;
import com.koverse.sdk.data.SimpleRecord;
import com.koverse.thrift.TConfigValue;
import com.koverse.thrift.TConfigValueType;
import com.koverse.thrift.TNotFoundException;
import com.koverse.thrift.client.Client;
import com.koverse.thrift.client.ClientConfiguration;
import com.koverse.thrift.collection.TCollection;
import com.koverse.thrift.collection.TIndexingPolicy;
import com.koverse.thrift.dataflow.TImportFlow;
import com.koverse.thrift.dataflow.TImportFlowType;
import com.koverse.thrift.dataflow.TJobAbstract;
import com.koverse.thrift.dataflow.TSource;
import com.koverse.thrift.dataflow.TTransform;
import com.koverse.thrift.dataflow.TTransformInputDataWindowType;
import com.koverse.thrift.dataflow.TTransformScheduleType;

import org.apache.thrift.TException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ExampleDataFlowAutomator {

  private static Client connect() throws IOException, TException {

    String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
    String appConfigPath = rootPath + "client.properties";

    Properties appProperties = new Properties();
    appProperties.load(new FileInputStream(appConfigPath));

    String host = appProperties.getProperty("koverse.host");
    String name = appProperties.getProperty("client.name");
    String secret = appProperties.getProperty("client.secret");

    if(host.isEmpty() || name.isEmpty() || secret.isEmpty()) {
      throw new IllegalArgumentException("You must update the client.properties file before running this example.");
    }

    ClientConfiguration config =
        ClientConfiguration.builder().host(host).clientName(name).clientSecret(secret).build();

    return new Client(config);
  }

  public static void setupDataFlow(Client client, String dataFlowName, String pages) throws TException {

    // First we need a source for our data
    System.out.println("setting up source");
    TSource tSource =  getExampleSource(client, dataFlowName, pages);

    // Set up the DataSet (TCollection) object
    System.out.println("setting up data set");
    TCollection dataSet = client.createDataSet(dataFlowName);

    // Set the indexing policy
    TIndexingPolicy tIndexingPolicy = new TIndexingPolicy();
    tIndexingPolicy.setForeignLanguageIndexing(false);
    dataSet.setIndexingPolicy(tIndexingPolicy);

    // Save the updated dataSet
    dataSet = client.updateDataSet(dataSet);

    // Next we need connect an import flow to pull in data
    System.out.println("setting up import flow");
    TImportFlow importFlow = new TImportFlow();

    importFlow.setSourceId(tSource.getSourceId());
    importFlow.setDataCollectionId(dataSet.getId());
    importFlow.setType(TImportFlowType.MANUAL);
    importFlow = client.createImportFlow(importFlow);

    // save import flow id back to dataset
    List<Long> importFlowIds = Arrays.asList(importFlow.getImportFlowId());
    dataSet.setImportFlowIds(importFlowIds);
    client.updateDataSet(dataSet);

    // setup analytical transform

    System.out.println("setting up derivative data set");
    TCollection wordCountDataSet = client.createDataSet(dataFlowName + " Keywords");

    System.out.println("setting up transform");
    TTransform transform = new TTransform();
    transform.setType("extract-keywords-transform");
    transform.setDisabled(false);
    // setting the schedule type to AUTOMATIC means the transform will run
    // whenever there is new data to process
    transform.setScheduleType(TTransformScheduleType.AUTOMATIC);
    transform.setInputDataWindowType(TTransformInputDataWindowType.NEW_DATA);
    transform.setReplaceOutputData(false);
    transform.setInputDataSlidingWindowOffsetSeconds(0);
    transform.setInputDataSlidingWindowSizeSeconds(0);

    // configure the transform to process text in the "article" field
    Map<String, TConfigValue> transformOptions = new HashMap<>();

    TConfigValue textFieldValue = new TConfigValue();
    textFieldValue.setType(TConfigValueType.STRING);
    textFieldValue.setStringValue("article");
    transformOptions.put("textFieldName", textFieldValue);

    TConfigValue titleFieldValue = new TConfigValue();
    titleFieldValue.setType(TConfigValueType.STRING);
    titleFieldValue.setStringValue("title");
    transformOptions.put("titleFieldName", titleFieldValue);

    TConfigValue numKeywordsValue = new TConfigValue();
    numKeywordsValue.setType(TConfigValueType.LONG);
    numKeywordsValue.setLongValue(20L);
    transformOptions.put("numKeywords", numKeywordsValue);

    // configure the transform to read from the Wikipedia articles data set
    TConfigValue inputCollectionValue = new TConfigValue();
    inputCollectionValue.setType(TConfigValueType.STRING_LIST);
    inputCollectionValue.setStringList(newArrayList(dataSet.getId()));
    transformOptions.put("inputCollection", inputCollectionValue);

    // configure the transform to write results to the word count data set
    TConfigValue outputCollectionValue = new TConfigValue();
    outputCollectionValue.setType(TConfigValueType.STRING);
    outputCollectionValue.setStringValue(wordCountDataSet.getId());
    transformOptions.put("outputCollection", outputCollectionValue);

    transform.setParameters(transformOptions);

    client.createTransform(transform);
  }


  public static void executeAndMonitorDataFlow(Client client, String dataFlowName) throws TException, InterruptedException {

    TCollection dataSet = client.getDataSetByName(dataFlowName);
    List<Long> importFlowIds = dataSet.getImportFlowIds();

    // start the import
    importFlowIds.forEach(ifid -> {
      try {
        System.out.println(String.format("executing data flow for %s %d", dataFlowName, ifid));
        client.executeImportFlow(ifid);
      } catch (TException ex) {
        System.out.println(ex.getMessage());
      }
    });

    // wait for import to complete
    waitForDataSetJobsToComplete(client, dataSet);

    TCollection resultsDataSet = client.getDataSetByName(dataFlowName + " Keywords");

    waitForDataSetJobsToComplete(client, resultsDataSet);
  }

  private static void waitForDataSetJobsToComplete(Client client, TCollection dataSet)
      throws TException, InterruptedException {

    System.out.println(String.format("Waiting for jobs to start for data set %s ..", dataSet.getName()));
    Set<Long> jobIds = new HashSet<>();
    List<TJobAbstract> jobs = client.getAllActiveJobs(dataSet.getId());

    // we'll wait until the import job we requested starts
    while (jobs.isEmpty()) {
      Thread.sleep(2000);
      jobs = client.getAllActiveJobs(dataSet.getId());
    }

    System.out.println(String.format("got %d jobs running", jobs.size()));
    System.out.println("waiting for jobs to complete");

    // now we'll wait until the import job, background processing jobs, and transform job are completed
    while (!jobs.isEmpty()) {
      Thread.sleep(5000);
      jobs = client.getAllActiveJobs(dataSet.getId());
      for (TJobAbstract job : jobs) {
        jobIds.add(job.getId());
        System.out.println(String.format("Job %d %s: %s", job.getId(), job.getType(), job.getStatus()));
      }
    }

    System.out.println("jobs completed");

    // check for any jobs that errored out
    for (Long jobId : jobIds) {
      TJobAbstract job = client.getJob(jobId);
      if (job.getStatus().equals("error")) {
        System.out.println(String.format("Job completed with status error: %n%n %s", job.getErrorDetail()));
      }
    }
  }

  public static void tearDownDataFlow(Client client, String dataFlowName) throws TException {

    try {
      TCollection dataSet = client.getDataSetByName(dataFlowName);

      // deleting this data set will delete associated sources and transforms
      System.out.println("Deleting wikipedia pages data set ...");
      client.deleteDataSet(dataSet.getId());
    } catch (TNotFoundException tnfe) {
      // nothing to remove .. so skip
    }

    try {
      TCollection keywordDataSet = client.getDataSetByName(dataFlowName + " Keywords");

      System.out.println("Deleting keyword data set ...");
      client.deleteDataSet(keywordDataSet.getId());
    } catch (TNotFoundException tnfe) {
      // nothing to remove
    }

    System.out.println(String.format("Done tearing down data flow: %s", dataFlowName));
  }

  private static void previewOutput(Client client, String dataFlowName) throws TException {

    List<DataSetResult> results = client.luceneQuery(
        "title: Odin",
        newArrayList(dataFlowName + " Keywords"),
        0,
        0,
        Collections.emptyList(),
        Collections.emptyList());

    for (DataSetResult result : results) {
      System.out.println(String.format("Found %d results in data set: %s", result.getRecordMatchCount(), result.getName()));

      for (SimpleRecord record : result.getRecords()) {
        System.out.println(record.get("word") + " " + record.get("score"));
      }
    }
  }

  private static TSource getExampleSource (Client client, String dataFlowName, String pages) throws TException {
    TSource wikipediaSource = new TSource();
    wikipediaSource.setName(dataFlowName + " Wikipedia Source");
    wikipediaSource.setTypeId("wikipedia-pages-source");

    Map<String, String> importOptions = new HashMap<>();
    importOptions.put("pageTitleListParam", pages);
    wikipediaSource.setParameters(importOptions);

    // this will fill out the ID of the source
    wikipediaSource = client.createSource(wikipediaSource);

    return wikipediaSource;
  }

  public static void main(String[] args) throws TException, IOException, InterruptedException {

    String dataFlowName = "Example Data Flow";
    String pages = "Thor Odin Freyja";
    Client client = connect();

    tearDownDataFlow(client, dataFlowName);
    setupDataFlow(client, dataFlowName, pages);
    executeAndMonitorDataFlow(client, dataFlowName);
    previewOutput(client, dataFlowName);
  }

}
