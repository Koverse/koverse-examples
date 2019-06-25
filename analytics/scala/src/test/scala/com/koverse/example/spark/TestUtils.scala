package com.koverse.example.spark

import com.koverse.thrift.client.{Client, ClientConfiguration}
import com.koverse.thrift.{TConfigValue, TNotFoundException}
import com.koverse.thrift.collection.TCollection
import com.koverse.thrift.dataflow.{TImportFlow, TImportFlowType, TJobAbstract, TSource, TTransform, TTransformInputDataWindowType, TTransformScheduleType}
import java.io.FileInputStream
import java.util
import java.util.{HashMap, HashSet, Map, Properties, Set}

import org.apache.thrift.TException

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait TestUtils {

  val client = connect

  protected def connect: Client = {
    val rootPath = Thread.currentThread.getContextClassLoader.getResource("").getPath
    val appConfigPath = rootPath + "client.properties"
    val appProperties = new Properties
    appProperties.load(new FileInputStream(appConfigPath))
    val host = appProperties.getProperty("koverse.host")
    val name = appProperties.getProperty("client.name")
    val secret = appProperties.getProperty("client.secret")
    if (host.isEmpty || name.isEmpty || secret.isEmpty)
      throw new IllegalArgumentException("You must update the client.properties file before running this example.")
    val config = ClientConfiguration.builder.host(host).clientName(name).clientSecret(secret).build
    new Client(config)
  }

  protected def createDataSet(name: String): TCollection = client.createDataSet(name)

  protected def setUpImport(importDataSet: TCollection, pages: String): TCollection = {
    val wikipediaSource = new TSource
    wikipediaSource.setName(importDataSet.getName + " Wikipedia WikipediaSourceUtil")
    wikipediaSource.setTypeId("wikipedia-pages-source")
    val importOptions = new util.HashMap[String, String]
    importOptions.put("pageTitleListParam", pages)
    wikipediaSource.setParameters(importOptions)
    // this will fill out the ID of the source
    val wikipediaSourceId = client.createSource(wikipediaSource).getSourceId
    var importFlow = new TImportFlow
    importFlow.setSourceId(wikipediaSourceId)
    importFlow.setDataCollectionId(importDataSet.getId)
    importFlow.setType(TImportFlowType.MANUAL)
    importFlow = client.createImportFlow(importFlow)
    // save import flow id back to dataset
    val importFlowId:java.lang.Long = importFlow.getImportFlowId
    val importFlowIds = List(importFlowId)
    importDataSet.setImportFlowIds(importFlowIds.asJava)
    importDataSet.setImportFlowId(importFlow.getImportFlowId)
    client.updateDataSet(importDataSet)
  }

  protected def configureAndSaveTransform(transformType: String, transformOptions: util.Map[String, TConfigValue]) = {
    println("setting up transform: " + transformType)
    val transform = new TTransform
    transform.setType(transformType)
    transform.setDisabled(false)
    // setting the schedule type to AUTOMATIC means the transform will run
    // whenever there is new data to process
    transform.setScheduleType(TTransformScheduleType.AUTOMATIC)
    transform.setInputDataWindowType(TTransformInputDataWindowType.NEW_DATA)
    transform.setReplaceOutputData(false)
    transform.setInputDataSlidingWindowOffsetSeconds(0)
    transform.setInputDataSlidingWindowSizeSeconds(0)
    // configure the transform to process text in the "article" field
    transform.setParameters(transformOptions)
    client.createTransform(transform)
  }

  protected def executeAndMonitorImportFlow(dataSetId: String, transformDataSetId: String) = {
    val dataSet = client.getDataSet(dataSetId)
    executeImportFlow(client, dataSet)
    // wait for import to complete
    checkJobProgress(dataSet.getId, transformDataSetId)
  }

  private def executeImportFlow(client: Client, dataSet: TCollection) = {
    val importFlowIds = dataSet.getImportFlowIds.asScala
    // start the import
    importFlowIds.foreach( id => {
      println(s"executing data flow for ${dataSet.getName} $id")
      client.executeImportFlow(id)
    })
  }

  private def checkJobProgress(dataSetId: String, transformDataSetId: String) = {
    val jobIds = new util.HashSet[Long]
    val jobStatus = new util.HashMap[String, String]
    println("Monitoring jobs for " + dataSetId)
    // we'll wait until the import job we requested starts
    // but only wait 1 minute in case it's finished before we get here
    try {
      var jobs = client.getAllActiveJobs(dataSetId)
      jobs.addAll(client.getAllActiveJobs(transformDataSetId))
      while ( {
        jobs.isEmpty
      }) {
        Thread.sleep(2000)
        System.out.println("waiting for jobs to start for " + dataSetId)
        jobs = client.getAllJobs(dataSetId)
        jobs.addAll(client.getAllActiveJobs(transformDataSetId))
      }
    } catch {
      case ex@(_: InterruptedException | _: TException) =>
        System.out.println(ex.getMessage)
    }
    val allJobs = client.getAllJobs(dataSetId).asScala
    for (alljob <- allJobs) {
      if ((alljob.getStatus eq "success") || (alljob.getStatus eq "error")) jobIds.add(alljob.getId)
      else {
        var allActiveJobs = client.getAllActiveJobs(dataSetId)
        allActiveJobs.addAll(client.getAllActiveJobs(transformDataSetId))
        // while there are jobs running we want to keep updating our map of statuses
        while ( {
          !allActiveJobs.isEmpty
        }) {
          Thread.sleep(5000)
          allActiveJobs = client.getAllActiveJobs(dataSetId)
          allActiveJobs.addAll(client.getAllActiveJobs(transformDataSetId))
          import scala.collection.JavaConversions._
          for (job <- allActiveJobs) {
            jobIds.add(job.getId)
            // keep for troubleshooting
            //            System.out.println(String.format(
            //            "%s %d %s:%s %f", dataSetId, job.getId(), job.getType(), job.getStatus(), job.getProgress()));
          }
          for (jobId <- jobIds) {
            val job = client.getJob(jobId)
            if (job.getStatus == "error")
              println(s"${job.getType} ${job.getId} completed with status error: ${job.getErrorDetail}")
          }
        }
      }
    }
    println("Jobs completed.")

    val it = jobIds.iterator()
    while(it.hasNext) {
      val id = it.next()
      try {
        val tJobAbstract = client.getJob(id)
        println("Job " + id + ", " + tJobAbstract.getStatus)
        jobStatus.put(tJobAbstract.getType.toString, tJobAbstract.getStatus)
      } catch {
        case ex: TException =>
          jobStatus.put("ERROR", "exception")
      }
    }
  }


  protected def tearDownDataFlow(dataFlowName: String) = {
    try {
      val dataSet = client.getDataSetByName(dataFlowName)
      // deleting this data set will delete associated sources and transforms
      println("Deleting wikipedia pages data set ...")
      client.deleteDataSet(dataSet.getId)
    } catch {
      case tnfe: TNotFoundException =>
      // nothing to remove .. so skip
    }

    try {
      val keywordDataSet = client.getDataSetByName(dataFlowName + " Keywords")
      println("Deleting keyword data set ...")
      client.deleteDataSet(keywordDataSet.getId)
    } catch {
      case tnfe: TNotFoundException =>
      // nothing to remove
    }
    println(String.format("Done tearing down data flow: %s", dataFlowName))
  }

}

