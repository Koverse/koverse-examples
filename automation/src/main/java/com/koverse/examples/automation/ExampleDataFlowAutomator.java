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

import com.koverse.examples.analytics.SentimentAnalysis;
import com.koverse.examples.integration.ExampleUrlSource;

import com.koverse.client.thrift.Client;
import com.koverse.client.thrift.ClientConfiguration;
import com.koverse.client.thrift.KTConnection;
import com.koverse.thrift.TConfigValue;
import com.koverse.thrift.TConfigValueType;
import com.koverse.thrift.collection.TCollection;
import com.koverse.thrift.collection.TIndexingPolicy;
import com.koverse.thrift.dataflow.TImportFlow;
import com.koverse.thrift.dataflow.TImportFlowSchedule;
import com.koverse.thrift.dataflow.TImportFlowType;
import com.koverse.thrift.dataflow.TJobAbstract;
import com.koverse.thrift.dataflow.TSource;
import com.koverse.thrift.dataflow.TTransform;
import com.koverse.thrift.dataflow.TTransformInputDataWindowType;
import com.koverse.thrift.dataflow.TTransformScheduleType;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ExampleDataFlowAutomator {

  private static Client connect() throws IOException, TException {

    String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
    String appConfigPath = rootPath + "client.properties";

    Properties appProperties = new Properties();
    appProperties.load(new FileInputStream(appConfigPath));

    String host = appProperties.getProperty("koverse.host");
    String name = appProperties.getProperty("client.name");
    String secret = appProperties.getProperty("client.secret");

    if(host.isEmpty() || name.isEmpty() || secret.isEmpty()){
      throw new IllegalArgumentException("You must update the application.properties file before running this example.");
    }

    return connect(host, name, secret);
  }

  private static Client connect(String host, String clientName, String clientSecret) throws TException {
    ClientConfiguration config =
        ClientConfiguration.builder().host(host).clientName(clientName).clientSecret(clientSecret).build();
    KTConnection conn = new KTConnection(config);
    return new Client(conn);
  }

  public static void setupDataFlow(Client client, String dataFlowName) throws TException {

    log.info("setting up source");
    TSource tSource =  getExampleSource(client, dataFlowName);

//    TSource tSource = ExampleWikipediaSource.getSource(client);

    log.info("setting up data set");
    TCollection dataSet = client.createDataSet(dataFlowName);
    TIndexingPolicy tIndexingPolicy = new TIndexingPolicy();
    tIndexingPolicy.setForeignLanguageIndexing(false);
    dataSet.setIndexingPolicy(tIndexingPolicy);
    dataSet = client.updateDataSet(dataSet);

    log.info("setting up import flow");
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

    log.info("setting up derivative data set");
    TCollection sentimentDataSet = client.createDataSet(dataFlowName + " Sentiment");

    log.info("setting up transform");
    TTransform transform = new TTransform();
    transform.setType(SentimentAnalysis.ANALYZE_SENTIMENT_TRANSFORM);
    transform.setDisabled(false);
    transform.setScheduleType(TTransformScheduleType.AUTOMATIC);
    transform.setInputDataWindowType(TTransformInputDataWindowType.ALL_DATA);
    transform.setReplaceOutputData(true);
    transform.setInputDataSlidingWindowOffsetSeconds(0);
    transform.setInputDataSlidingWindowSizeSeconds(0);

    Map<String, TConfigValue> transformOptions = new HashMap<>();

    TConfigValue inputCollectionValue = new TConfigValue();
    inputCollectionValue.setType(TConfigValueType.STRING_LIST);
    inputCollectionValue.setStringList(newArrayList(dataSet.getId()));
    transformOptions.put("inputCollection", inputCollectionValue);

    TConfigValue outputCollectionValue = new TConfigValue();
    outputCollectionValue.setType(TConfigValueType.STRING);
    outputCollectionValue.setStringValue(sentimentDataSet.getId());
    transformOptions.put("outputCollection", outputCollectionValue);

    transform.setParameters(transformOptions);

    client.createTransform(transform);
  }


  public static void executeAndMonitorDataFlow(Client client, String dataFlowName) throws TException, InterruptedException {

    TCollection dataSet = client.getDataSetByName(dataFlowName);
    List<Long> importFlowIds = dataSet.getImportFlowIds();

    // start the import
    importFlowIds.forEach(id -> {
      try {
        log.info("executing data flow for {} {}", dataFlowName, id);
        client.executeImportFlowById(id);
      } catch (TException ex) {
        System.out.println(ex.getMessage());
      }
    });

    log.info("waiting for jobs to start ..");
    List<TJobAbstract> jobs = client.getJobsByDataSetId(dataSet.getId());

    while (jobs.isEmpty()) {
      Thread.sleep(2000);
      jobs = client.getJobsByDataSetId(dataSet.getId());
    }

    log.info("got {} jobs running", jobs.size());
    log.info("waiting for jobs to complete");

    while (!jobs.isEmpty()) {
      Thread.sleep(5000);
      jobs = client.getJobsByDataSetId(dataSet.getId());
      System.out.println(jobs.get(0).getStatus());
    }

    log.info("jobs completed");
  }

  public static void tearDownDataFlow(Client client, String dataFlowName) throws TException {

    TCollection dataSet = client.getDataSetByName(dataFlowName);
    TCollection sentimentDataSet = client.getDataSetByName(dataFlowName + " Sentiment");

    client.deleteDataSet(dataSet.getId());

    client.deleteDataSet(sentimentDataSet.getId());
  }

  public void shutdownImport(Client client, String dataSetName) throws TException{
    TCollection dataSet = client.getDataSetByName(dataSetName);
    List<Long> ids = dataSet.getImportFlowIds();
    try {
      for (int i = 0; i < ids.size(); i++) {
        List<TImportFlowSchedule> scheduleIds = client
            .getImportFlowSchedulesByImportFlowId(ids.get(i));
        for (int j = 0; j < scheduleIds.size(); j++) {
          client.deleteImportFlowSchedule(scheduleIds.get(j).getImportFlowId());
        }
      }
    } catch (NullPointerException np){
      //
    }
  }

  private static TSource getExampleSource (Client client, String dataFlowName) throws TException {
    TSource urlSource = new TSource();
    String sourceURL = "";
    urlSource.setName(dataFlowName + " URL Source");
    urlSource.setTypeId(ExampleUrlSource.URL_SOURCE_TYPE_ID);

    Map<String, String> importOptions = new HashMap<>();
    importOptions.put(ExampleUrlSource.URLS_PARAMETER, sourceURL);
    urlSource.setParameters(importOptions);

    urlSource = client.createSourceInstance(urlSource);

    return urlSource;
  }

  public static void main(String[] args) throws TException, IOException, InterruptedException {
    String dataFlowName = "exampleDataSet";

    Client client = connect();

    setupDataFlow(client, dataFlowName);
    executeAndMonitorDataFlow(client, dataFlowName);
    tearDownDataFlow(client, dataFlowName);
  }

}
