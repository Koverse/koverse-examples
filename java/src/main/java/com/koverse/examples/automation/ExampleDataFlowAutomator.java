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

import com.koverse.client.thrift.Client;
import com.koverse.client.thrift.ClientConfiguration;
import com.koverse.client.thrift.KTConnection;
import com.koverse.com.google.common.io.Resources;
import com.koverse.examples.analytics.SentimentAnalysis;
import com.koverse.examples.integration.ExampleUrlSource;
import com.koverse.thrift.TConfigValue;
import com.koverse.thrift.TConfigValueType;
import com.koverse.thrift.collection.TCollection;
import com.koverse.thrift.dataflow.TImportFlow;
import com.koverse.thrift.dataflow.TImportFlowType;
import com.koverse.thrift.dataflow.TJobAbstract;
import com.koverse.thrift.dataflow.TSource;
import com.koverse.thrift.dataflow.TTransform;
import com.koverse.thrift.dataflow.TTransformInputDataWindowType;
import com.koverse.thrift.dataflow.TTransformScheduleType;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ExampleDataFlowAutomator {

  private final String dataFlowName;
  private final Client client;
  private final String sourceURL;

  public ExampleDataFlowAutomator(String dataFlowName, String sourceURL, InputStream propertiesInputStream) throws IOException, TException {

    this.dataFlowName = dataFlowName;
    this.sourceURL = sourceURL;

    Properties properties = new Properties();

    // load a properties file
    properties.load(propertiesInputStream);

    ClientConfiguration config = ClientConfiguration.builder()
        .clientName(properties.getProperty("clientName"))
        .clientSecret(properties.getProperty("clientSecret"))
        .build();

    KTConnection conn = new KTConnection(config);
    client = new Client(conn);
  }

  public void setupDataFlow() throws TException {

    log.info("setting up source");
    TSource urlSource = new TSource();

    urlSource.setName(dataFlowName + " URL Source");
    urlSource.setTypeId(ExampleUrlSource.URL_SOURCE_TYPE_ID);

    Map<String, String> importOptions = new HashMap<>();
    importOptions.put(ExampleUrlSource.URLS_PARAMETER, sourceURL);
    urlSource.setParameters(importOptions);

    urlSource = client.createSourceInstance(urlSource);

    log.info("setting up data set");
    TCollection dataSet = client.createDataSet(dataFlowName);

    log.info("setting up import flow");
    TImportFlow importFlow = new TImportFlow();

    importFlow.setSourceId(urlSource.getSourceId());
    importFlow.setDataCollectionId(dataSet.getId());
    importFlow.setType(TImportFlowType.MANUAL);

    client.createImportFlow(importFlow);

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

  public void executeAndMonitorDataFlow() throws TException, InterruptedException {

    TCollection dataSet = client.getDataSetByName(dataFlowName);

    long importFlowId = dataSet.getImportFlowId();

    log.info("executing data flow");
    client.executeImportFlowById(importFlowId);

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
    }

    log.info("jobs completed");
  }

  public void tearDownDataFlow() throws TException {

    TCollection dataSet = client.getDataSetByName(dataFlowName);

    client.deleteDataSet(dataFlowName);

    client.deleteDataSet(dataFlowName + " Sentiment");
  }

  public static void main(String[] args) throws TException, IOException {

    final String propertiesFilename = "client.properties";
    final URL propertiesUrl = Resources.getResource(propertiesFilename);

    ExampleDataFlowAutomator automator = new ExampleDataFlowAutomator("test data", "", propertiesUrl.openStream());

    automator.setupDataFlow();

    //automator.executeAndMonitorDataFlow();

    //automator.tearDownDataFlow();
  }

}
