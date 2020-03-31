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
import com.koverse.thrift.TSchedule;
import com.koverse.thrift.TScheduleEndingType;
import com.koverse.thrift.TScheduleRepeatUnit;
import com.koverse.thrift.client.Client;
import com.koverse.thrift.client.ClientConfiguration;
import com.koverse.thrift.collection.TCollection;
import com.koverse.thrift.collection.TIndexingPolicy;
import com.koverse.thrift.dataflow.TExportJob;
import com.koverse.thrift.dataflow.TExportSchedule;
import com.koverse.thrift.dataflow.TImportFlow;
import com.koverse.thrift.dataflow.TImportFlowType;
import com.koverse.thrift.dataflow.TJobAbstract;
import com.koverse.thrift.dataflow.TSink;
import com.koverse.thrift.dataflow.TSinkTypeDescription;
import com.koverse.thrift.dataflow.TSource;
import com.koverse.thrift.dataflow.TTransform;
import com.koverse.thrift.dataflow.TTransformInputDataWindowType;
import com.koverse.thrift.dataflow.TTransformScheduleType;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.joda.time.DateTime;

public class ExampleDataFlowAutomator {

  private static Client connect() throws IOException, TException, NumberFormatException {

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

  public static TCollection setupDataFlow(Client client, String dataFlowName, String pages) throws TException {

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

    return dataSet;
  }


  public static TCollection executeAndMonitorDataFlow(Client client, String dataFlowName) throws TException, InterruptedException {

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
    waitForDataSetJobsToComplete(client, dataSet.getId());

    TCollection resultsDataSet = client.getDataSetByName(dataFlowName + " Keywords");

    waitForDataSetJobsToComplete(client, resultsDataSet.getId());

    return resultsDataSet;
  }

  public static long setupExport(Client client, String dataFlowName) throws TException {
    // using the HDFS output Sink
    String typeId = "hdfs-sink";

    // getting the sinkDescription for type hdfs-sink
    TSinkTypeDescription sinkDesc;
    try {
      sinkDesc=client.getSinkDescription(typeId);
    } catch (Exception ex) {
      System.out.println("sinkDesc");
      ex.printStackTrace();
    }

    TSink sink = new TSink();
    sink.setName(dataFlowName + "sink");

    sink.setType(typeId);
    System.out.println("Setting up export " + dataFlowName + " " + typeId);
    Map<String, String> sinkParams = new HashMap<>();
    sinkParams.put("hdfs_namenode", "koversevm:8020");
    sinkParams.put("file_directory", "koverse-examples");
    sinkParams.put("koverse_exportfileformat_csv_fieldnames", "title article revision timestamp");
    sinkParams.put("single_output", "true");
    sinkParams.put("inputCollection", dataFlowName);
    sinkParams.put("file_prefix", "example");
    sinkParams.put("file_format_type", "koverse_exportfileformat_json");
    sink.setParameters(sinkParams);
    sink.setScheduleType("automatic");
    sink.setInputDataWindowType("allData");

    long sinkId =  client.createSink(sink).getId();

    TExportSchedule exportSchedule = new TExportSchedule();
    exportSchedule.setSinkId(sinkId);
    TSchedule schedule = new TSchedule();
    schedule.setStartsOnMs(DateTime.now().plusSeconds(20).getMillis());
    schedule.setRepeatUnit(TScheduleRepeatUnit.Monthly);
    schedule.setEndingType(TScheduleEndingType.Count);
    schedule.setEndsAfter(1);
    schedule.setCron("0 0 1 * *");

    exportSchedule.setSchedule(schedule);

    client.createExportSchedule(exportSchedule);

    System.out.println(client.getSink(sinkId).toString());
    System.out.println(exportSchedule.toString());

    return sinkId;
  }

  public static TExportJob startSink(Client client, long sinkId) throws TException {
    TExportJob exportJob = new TExportJob();
    try{
      exportJob = client.executeSink(sinkId);
      System.out.println("sink started " + exportJob.toString());
    } catch (Exception ex){
      System.out.println(ex.getMessage());
      System.out.println(ex.getCause());
      ex.printStackTrace();
    }
    try {
      waitForDataSetJobsToComplete(client, exportJob.getSourceCollectionId());
    } catch (InterruptedException ex) {
      System.out.println(ex.getMessage());
    }

   return exportJob;
  }

  private static void waitForDataSetJobsToComplete(Client client, String dataSetId)
      throws TException, InterruptedException {

    Set<Long> jobIds = new HashSet<>();
    List<TJobAbstract> activeJobs = client.getAllActiveJobs(dataSetId);

    Map<String, String> jobStatus = new HashMap<>();

    System.out.println("Monitoring jobs for " + dataSetId);
    // we'll wait until the import job we requested starts
    while (activeJobs.isEmpty()) {
      Thread.sleep(2000);
      activeJobs = client.getAllActiveJobs(dataSetId);
    }

    // while there are jobs running we want to keep updating our map of statuses
    while (!activeJobs.isEmpty()) {
      Thread.sleep(5000);
      activeJobs = client.getAllActiveJobs(dataSetId);
      for (TJobAbstract job : activeJobs) {
        jobIds.add(job.getId());
         System.out.println(String.format("%d %s:%s", job.getId(), job.getType(), job.getStatus()));
      }
      for (Long jobId : jobIds) {
        TJobAbstract job = client.getJob(jobId);
        if (job.getStatus().equals("error")) {
          System.out.println(String.format("Job completed with status error: %n%n %s", job.getErrorDetail()));
        }
      }
    }

    System.out.println("Jobs completed.");

    jobIds.forEach((id) -> {
      String finalStatus;
      String jobType;
      try {
        TJobAbstract tJobAbstract = client.getJob(id);
        finalStatus = tJobAbstract.getStatus();
        jobType = tJobAbstract.getType().name();
         System.out.println("Job " + id + ", " + finalStatus);
      } catch (TException ex) {
        finalStatus = "exception";
        jobType = "ERROR";
      }
      jobStatus.put(jobType, finalStatus);
    });

  }

  public static void tearDownDataFlow(Client client, String dataFlowName, boolean removeOutput) throws TException {

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

    if(removeOutput) {
      try {
        // remove hdfs file
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost"), config);
        boolean isRecursive = true;
        Path path = new Path("/user/koverse/" + "koverse-examples");
        hdfs.delete(path, isRecursive);
      } catch (IOException | URISyntaxException ex) {
        System.out.println("Unable to remove hdfs files " + ex.getMessage());
      }
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
    boolean removeOutput = true; // remove the hdfs files created by the export process, set to false to keep the files.

    // remove the data sets and files created by the last test
    tearDownDataFlow(client, dataFlowName, removeOutput);

    // set up the import data flow
    TCollection orgDataSet = setupDataFlow(client, dataFlowName, pages);

    // set up the keywords tranform data flow
    TCollection keywordsDataSet = executeAndMonitorDataFlow(client, dataFlowName);

    // show the output from the keywords transform
    previewOutput(client, dataFlowName);

    // set up export to hdfs
    long sinkId = setupExport(client, orgDataSet.getId());
    startSink(client, sinkId);

  }

}
