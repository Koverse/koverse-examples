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

package com.koverse.example.spark

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import com.koverse.sdk.data.SimpleRecord
import org.apache.spark.sql.SparkSession
import org.junit.Assert

import scala.collection.JavaConverters._

/**
 * These tests leverage the great work at https://github.com/holdenk/spark-testing-base
 */
@RunWith(classOf[JUnitRunner])
class WordCounterTest extends FunSuite {

  val sparkSession = SparkSession
    .builder()
    .appName("WordCounterDataFrameTest")
    .master("local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  test("RDD test") {
    val inputRecords = List(
        new SimpleRecord(Map[String,Object]("text" -> "these words are to be counted", "id" -> "0").asJava),
        new SimpleRecord(Map[String,Object]("text" -> "more words    that are worth counting", "id" -> "1").asJava))

    val inputRecordsRdd = sc.parallelize(inputRecords)
    val wordCounter = new WordCounter("text", """['".?!,:;\s]+""")
    val outputRecordsRdd = wordCounter.count(inputRecordsRdd)

    Assert.assertEquals(outputRecordsRdd.count, 10)
    val outputRecords = outputRecordsRdd.collect()
    val countRecordOption = outputRecords.find { simpleRecord => simpleRecord.get("word").equals("are") }

    Assert.assertTrue(countRecordOption.isDefined)
    Assert.assertEquals(countRecordOption.get.get("count"), 2)
  }

  test("DataFrame test") {

    val messages = List(
        Message("these words are to be counted", "title", 0),
        Message("more words that are worth counting", "title1", 1))

    val inputDataFrame = sparkSession.createDataFrame(messages)
    val wordCounter = new WordCounter("article", """['".?!,:;\s]""")
    val outputDataFrame = wordCounter.count(inputDataFrame)

    Assert.assertEquals(outputDataFrame.count(), 10)
    val outputRows = outputDataFrame.collect()
    val countRowOption = outputRows.find { row => row.getAs[String]("lowerWord").equals("are") }
    Assert.assertTrue(countRowOption.isDefined)
    Assert.assertEquals(countRowOption.get.getAs[Long]("count"), 2)

  }

  test("Dataset test no matching fields") {
    import sparkSession.implicits._

    val messages = Seq(
      Message("these words are to be counted", "words", 0),
      Message("more words that are worth counting", "more words", 1))

    val inputDataset = messages.toDS()//sparkSession.createDataset(messages)//.as[Message](messageEncoder)
    val wordCounter = new WordCounter("text", """['".?!,:;\s]""")
    val outputDataset = wordCounter.count(inputDataset, sparkSession)

    Assert.assertEquals(outputDataset.count(), 0)

  }
}
