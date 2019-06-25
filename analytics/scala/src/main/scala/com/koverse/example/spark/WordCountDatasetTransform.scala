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

import com.koverse.sdk.Version
import com.koverse.sdk.data.Parameter
import com.koverse.sdk.transform.scala.{DatasetTransform, DatasetTransformContext}
import org.apache.spark.sql.Dataset


case class Message(text: String, id: String)
case class WordCount(text: String, count: Int)

class WordCountDatasetTransform extends DatasetTransform {

  private val TEXT_FIELD_NAME_PARAMETER = "textFieldName"
  private val INPUT_DATA_SET_PARAMETER = "inputDataSet"

  /**
    * Koverse calls this method to execute your transform.
    *
    * @param context The context of this spark execution
    * @return The resulting Dataset of this transform execution.
    *         It will be applied to the output collection.
    */
  override def execute(context: DatasetTransformContext): Dataset[WordCount] = {

    // for each Record, tokenize the specified text field and count each occurence
    val input = context.getDatasets.get(INPUT_DATA_SET_PARAMETER).get.asInstanceOf[Dataset[Message]]
    val textFieldName = context.getParameters.get(TEXT_FIELD_NAME_PARAMETER)
    
    // Create the WordCounter which will perform the logic of our Transform
    val tokenizationString =
      """['".?!,:;\s]+"""

    // Create the WordCounter which will perform the logic of our Transform
    val wordCounter = new WordCounter(textFieldName.get, """['".?!,:;\s]+""")
    wordCounter.count(input, context.getSparkSession)

  }

  /*
   * The following provide metadata about the Transform used for registration
   * and display in Koverse.
   */

  /**
    * Get the name of this transform. It must not be an empty string.
    *
    * @return The name of this transform.
    */
  override def getName: String = "Word Count Dataset Example"

  /**
    * Get the parameters of this transform.  The returned iterable can
    * be immutable, as it will not be altered.
    *
    * @return The parameters of this transform.
    */
  /**
    * Get the parameters of this transform.  The returned iterable can
    * be immutable, as it will not be altered.
    *
    * @return The parameters of this transform.
    */
  override def getParameters: scala.collection.immutable.Seq[Parameter] = {

    // This parameter will allow the user to input the field name of their Records which
    // contains the strings that they want to tokenize and count the words from. By parameterizing
    // this field name, we can run this Transform on different Records in different Collections
    // without changing the code
    val textParameter = Parameter.newBuilder
      .parameterName(TEXT_FIELD_NAME_PARAMETER)
      .displayName("Text Field Name")
      .`type`(Parameter.TYPE_STRING)
      .defaultValue("")
      .required(true)
      .parameterGroup("myGroup")
      .build

    val inputDatasetParameter = Parameter.newBuilder
      .parameterName(INPUT_DATA_SET_PARAMETER)
      .displayName("Dataset containing input records")
      .`type`(Parameter.TYPE_INPUT_COLLECTION)
      .required(true)
      .parameterGroup("myGroup")
      .build

    scala.collection.immutable.Seq(textParameter, inputDatasetParameter)
  }

  /**
    * Get the programmatic identifier for this transform.  It must not
    * be an empty string and must contain only alpha numeric characters.
    *
    * @return The programmatic id of this transform.
    */
  override def getTypeId: String = "wordCountDatasetExample"

  /**
    * Get the version of this transform.
    *
    * @return The version of this transform.
    */
  override def getVersion: Version = new Version(0, 0, 1)

  /**
    * Get the description of this transform.
    *
    * @return The the description of this transform.
    */
  override def getDescription: String = "This is the Word Count Dataset Example"

  override def supportsIncrementalProcessing(): Boolean = true

  override def getDatasetBeans: scala.collection.immutable.Map[String, AnyRef] = {
    val datasetBeans: scala.collection.immutable.Map[String, AnyRef] =
      scala.collection.immutable.HashMap(INPUT_DATA_SET_PARAMETER -> classOf[Message])
    datasetBeans
  }

}
