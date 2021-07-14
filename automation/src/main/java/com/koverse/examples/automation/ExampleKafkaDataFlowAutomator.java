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
import com.koverse.thrift.dataflow.TImportTransformConfiguration;
import com.koverse.thrift.dataflow.TJobAbstract;
import com.koverse.thrift.dataflow.TSource;
import com.koverse.thrift.dataflow.TTransform;
import com.koverse.thrift.dataflow.TTransformInputDataWindowType;
import com.koverse.thrift.dataflow.TTransformScheduleType;
import org.apache.thrift.TException;
import org.assertj.core.api.Assertions;

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
import java.util.UUID;

import static com.koverse.com.google.common.collect.Lists.newArrayList;

public class ExampleKafkaDataFlowAutomator {

  private static Client client;

  public static void main(String[] args) throws IOException, TException, InterruptedException {

    String dataFlowName = "Example Kafka Data Flow";
    String topic = "testTopic";

    client = connect();
    tearDownDataFlow(client, dataFlowName);
    setupDataFlow(client, dataFlowName, topic);
    executeAndMonitorDataFlow(client, dataFlowName);

  }

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

    ClientConfiguration config = ClientConfiguration.builder().host(host).clientName(name).clientSecret(secret).build();

    return new Client(config);
  }

  private static void setupDataFlow(Client client, String dataFlowName, String topic) throws TException, IOException {

    // Create KafkaUtil data source
    TSource kafkaSource = getKafkaSource(client, dataFlowName + " Kafka Source", topic);

    // Create data set
    TCollection dataSet = client.createDataSet(dataFlowName);

    // Setup data flow
    TImportFlow importFlow = createImportFlow(client, dataSet, kafkaSource.getSourceId());

    // Add normalization
    TImportTransformConfiguration normalization = createByteArrayToStringTransformForFlow(client, importFlow);
    importFlow.setTransforms(newArrayList(normalization));
    client.updateImportFlow(importFlow.getImportFlowId(), importFlow);

  }

  private static TSource getKafkaSource(Client client, String dataFlowName, String topic) throws TException, IOException {
    TSource kafkaSource = new TSource();
    kafkaSource.setName(dataFlowName);
    kafkaSource.setTypeId("kafka-0.10-source");
    Properties kafkaProperties = new Properties();
    kafkaProperties.setProperty("bootstrap.servers", "localhost:29092");
    kafkaProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    kafkaProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Map<String, String> importOptions = new HashMap<>();
    importOptions.put("bootstrapBrokers", kafkaProperties.getProperty("bootstrap.servers"));
    importOptions.put("group", UUID.randomUUID().toString());
    importOptions.put("topic", topic);
    importOptions.put("numberOfConsumers", "1");
    importOptions.put("auto.offset.reset", "earliest");
    kafkaSource.setParameters(importOptions);

    // this will fill out the ID of the source
    kafkaSource = client.createSource(kafkaSource);

    return kafkaSource;
  }

  public static TImportFlow createImportFlow(Client client, TCollection dataSet, long sourceId) throws TException {
    TImportFlow importFlow = new TImportFlow();

    importFlow.setSourceId(sourceId);
    importFlow.setDataCollectionId(dataSet.getId());
    importFlow.setType(TImportFlowType.MANUAL);
    importFlow = client.createImportFlow(importFlow);

    // save import flow id back to dataset
    List<Long> importFlowIds = newArrayList(importFlow.getImportFlowId());
    dataSet.setImportFlowIds(importFlowIds);
    dataSet.setImportFlowId(importFlow.getImportFlowId());
    client.updateDataSet(dataSet);
    return importFlow;
  }

  public static TImportTransformConfiguration createByteArrayToStringTransformForFlow(Client client, TImportFlow importFlow)
      throws TException {

    // Create a copy transform
    TImportTransformConfiguration normalization = new TImportTransformConfiguration();
    normalization.setTypeId("byteArrayToString");
    normalization.setImplementationClassName("com.koverse.addon.queues.importtransform.ByteArrayToStringImportTransform");
    normalization.setImportFlowId(importFlow.getImportFlowId());
    normalization.setEnabled(true);

    Map<String, TConfigValue> transformOptions = new HashMap<>();
    TConfigValue valueFieldName = new TConfigValue();
    valueFieldName.setType(TConfigValueType.STRING);
    valueFieldName.setStringValue("payload");
    transformOptions.put("valueFieldName", valueFieldName);

    TConfigValue removeOriginalField = new TConfigValue();
    removeOriginalField.setType(TConfigValueType.BOOLEAN);
    removeOriginalField.setBooleanValue(true);
    transformOptions.put("removeOriginalField", removeOriginalField);

    TConfigValue newFieldName = new TConfigValue();
    newFieldName.setType(TConfigValueType.STRING);
    newFieldName.setStringValue("payloadText");
    transformOptions.put("newFieldName", newFieldName);

    normalization.setConfig(transformOptions);
    return client.createNormalization(importFlow.getImportFlowId(), normalization);

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
  }

  private static void waitForDataSetJobsToComplete(Client client, TCollection dataSet)
      throws TException, InterruptedException {

    String dataSetId  = dataSet.getId();

    System.out.println(String.format("Waiting for jobs to start for data set %s ..", dataSet.getName()));
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
      System.out.println(String.format("got %d jobs running", activeJobs.size()));
      System.out.println("waiting for jobs to complete");

      // now we'll wait until the import job, background processing jobs, and transform job are completed
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
          System.out.println(String.format("Job %ds", id));
        }
        System.out.println("jobs completed");

        // check for any jobs that errored out
        for (Long jobId : jobIds) {
          try {
            TJobAbstract job = client.getJob(jobId);
            if (job.getStatus().equals("error")) {
              System.out.println(
                  String.format("Job completed with status error: %n%n %s", job.getErrorDetail()));
            }
            jobStatus.put(jobType, finalStatus);
          } catch (TException e) {
            System.out.println(e.getMessage());
          }
        }
      });
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

}
