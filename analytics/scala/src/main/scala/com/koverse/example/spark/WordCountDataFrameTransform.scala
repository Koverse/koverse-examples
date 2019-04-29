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
import com.koverse.sdk.transform.java.{DataFrameTransform, DataFrameTransformContext}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters.seqAsJavaListConverter

class WordCountDataFrameTransform extends DataFrameTransform {

  private val TEXT_FIELD_NAME_PARAMETER = "textFieldName"

  /**
    * Koverse calls this method to execute your transform.
    *
    * @param context The context of this spark execution
    * @return The resulting RDD of this transform execution.
    *         It will be applied to the output collection.
    */
  override def execute(context: DataFrameTransformContext): DataFrame = {

    // This transform assumes there is a single input Data Collection
    val inputCollectionId = context.getDatasetIds.get(0)

    // Get the RDD[SimpleRecord] that represents the input Data Collection
    val inputRecordsRdd = context.getDataFrames.get(inputCollectionId)

    // for each Record, tokenize the specified text field and count each occurence
    val textFieldName = context.getParameters().get(TEXT_FIELD_NAME_PARAMETER)

    // Create the WordCounter which will perform the logic of our Transform
    val wordCounter = new WordCounter(textFieldName, """['".?!,:;\s]+""")
    wordCounter.count(inputRecordsRdd)

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
  override def getName: String = "Word Count Example"

  /**
    * Get the parameters of this transform.  The returned iterable can
    * be immutable, as it will not be altered.
    *
    * @return The parameters of this transform.
    */
  override def getParameters: java.lang.Iterable[Parameter] = {

    // This parameter will allow the user to input the field name of their Records which
    // contains the strings that they want to tokenize and count the words from. By parameterizing
    // this field name, we can run this Transform on different Records in different Collections
    // without changing the code
    val textParameter = new Parameter(TEXT_FIELD_NAME_PARAMETER, "Text Field Name", Parameter.TYPE_STRING)
    Seq(textParameter).asJava
  }

  /**
    * Get the programmatic identifier for this transform.  It must not
    * be an empty string and must contain only alpha numeric characters.
    *
    * @return The programmatic id of this transform.
    */
  override def getTypeId: String = "wordCountExample"

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
  override def getDescription: String = "This is the Word Count Example"

  override def supportsIncrementalProcessing: Boolean = false
}
