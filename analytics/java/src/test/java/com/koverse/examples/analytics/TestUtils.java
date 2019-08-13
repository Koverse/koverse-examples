package com.koverse.examples.analytics;

import static com.koverse.com.google.common.collect.Lists.newArrayList;

import com.koverse.thrift.TConfigValue;
import com.koverse.thrift.TNotFoundException;
import com.koverse.thrift.client.Client;
import com.koverse.thrift.client.ClientConfiguration;
import com.koverse.thrift.collection.TCollection;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class TestUtils {
  Client client;

  TestUtils() {
    try {
      client = connect();
    } catch (IOException | TException ex) {
      System.out.println(ex.getMessage());
    }
  }

  protected Client connect() throws IOException, TException {

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

  protected TCollection createDataSet(String name) throws TException{
    return client.createDataSet(name);
  }

  protected TCollection setUpImport(TCollection importDataSet, String pages) throws TException {
    TSource wikipediaSource = new TSource();
    wikipediaSource.setName(importDataSet.getName() + " Wikipedia WikipediaSourceUtil");
    wikipediaSource.setTypeId("wikipedia-pages-source");

    Map<String, String> importOptions = new HashMap<>();
    importOptions.put("pageTitleListParam", pages);
    wikipediaSource.setParameters(importOptions);

    // this will fill out the ID of the source
    wikipediaSource = client.createSource(wikipediaSource);

    TImportFlow importFlow = new TImportFlow();

    importFlow.setSourceId(wikipediaSource.getSourceId());
    importFlow.setDataCollectionId(importDataSet.getId());
    importFlow.setType(TImportFlowType.MANUAL);
    importFlow = client.createImportFlow(importFlow);

    // save import flow id back to dataset
    List<Long> importFlowIds = newArrayList(importFlow.getImportFlowId());
    importDataSet.setImportFlowIds(importFlowIds);
    importDataSet.setImportFlowId(importFlow.getImportFlowId());

    return client.updateDataSet(importDataSet);
  }

  protected void configureAndSaveTransform(
      String transformType,
      Map<String, TConfigValue> transformOptions)
      throws TException {

    System.out.println("setting up transform: " + transformType);
    TTransform transform = new TTransform();

    transform.setType(transformType);
    transform.setDisabled(false);
    // setting the schedule type to AUTOMATIC means the transform will run
    // whenever there is new data to process
    transform.setScheduleType(TTransformScheduleType.AUTOMATIC);
    transform.setInputDataWindowType(TTransformInputDataWindowType.NEW_DATA);
    transform.setReplaceOutputData(false);
    transform.setInputDataSlidingWindowOffsetSeconds(0);
    transform.setInputDataSlidingWindowSizeSeconds(0);

    // configure the transform to process text in the "article" field

    transform.setParameters(transformOptions);

    client.createTransform(transform);

  }

  protected void executeAndMonitorImportFlow(String dataSetId, String transformDataSetId)
      throws TException, InterruptedException {

    TCollection dataSet = client.getDataSet(dataSetId);
    executeImportFlow(client, dataSet);
    // wait for import to complete
    checkJobProgress(dataSet.getId(), transformDataSetId);

  }

  private void executeImportFlow(Client client, TCollection dataSet) throws TException {
    List<Long> importFlowIds = dataSet.getImportFlowIds();

    // start the import


    importFlowIds.forEach (
      id -> {
        try {
          System.out.println(String.format("executing data flow for %s %d", dataSet.getName(), id));
          client.executeImportFlow(id);
        } catch (TException e) {
          System.out.println(e.getMessage());
        }
      });

  }

  private Map<String, String> checkJobProgress(String dataSetId, String transformDataSetId) throws TException, InterruptedException {
    Set<Long> jobIds = new HashSet<>();
    Map<String, String> jobStatus = new HashMap<>();

    System.out.println("Monitoring jobs for " + dataSetId);
    // we'll wait until the import job we requested starts
    // but only wait 1 minute in case it's finished before we get here

    try {
      List<TJobAbstract> jobs = client.getAllActiveJobs(dataSetId);
      jobs.addAll(client.getAllActiveJobs(transformDataSetId));
      while (jobs.isEmpty()) {
        Thread.sleep(2000);
        System.out.println("waiting for jobs to start for " + dataSetId);
        jobs = client.getAllJobs(dataSetId);
        jobs.addAll(client.getAllActiveJobs(transformDataSetId));
      }
    } catch (InterruptedException | TException ex) {
      System.out.println(ex.getMessage());
    }

    List<TJobAbstract> allJobs = client.getAllJobs(dataSetId);
    for (TJobAbstract alljob: allJobs) {
      if (alljob.getStatus() == "success" || alljob.getStatus() == "error") {
        jobIds.add(alljob.getId());
      } else {

        List<TJobAbstract> allActiveJobs = client.getAllActiveJobs(dataSetId);
        allActiveJobs.addAll(client.getAllActiveJobs(transformDataSetId));
        // while there are jobs running we want to keep updating our map of statuses
        while (!allActiveJobs.isEmpty()) {
          Thread.sleep(5000);
          allActiveJobs = client.getAllActiveJobs(dataSetId);
          allActiveJobs.addAll(client.getAllActiveJobs(transformDataSetId));
          for (TJobAbstract job : allActiveJobs) {
            jobIds.add(job.getId());
            // keep for troubleshooting
            //            System.out.println(String.format(
            //            "%s %d %s:%s %f", dataSetId, job.getId(), job.getType(), job.getStatus(), job.getProgress()));
          }
          for (Long jobId : jobIds) {
            TJobAbstract job = client.getJob(jobId);
            if (job.getStatus().equals("error")) {
              System.out.println(
                  String.format("%s %s completed with status error: %s", job.getType(), job.getId(), job.getErrorDetail()));
            }
          }
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

    return jobStatus;
  }


  protected void tearDownDataFlow(String dataFlowName) throws TException {

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
