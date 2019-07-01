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

package com.koverse.example.spark;

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import com.koverse.sdk.Version
import com.koverse.sdk.data.Parameter
import com.koverse.sdk.transform.scala.{DataFrameTransform, DataFrameTransformContext}



class LogisticRegressionDataPrep extends DataFrameTransform {

    /*
     * We’ll begin by defining a set of parameters that our Transform will use to request configuration information from a
     * user of the Koverse UI.
     */

    final val INPUT_DATASET = "inputDataSet"

    final val DATA_PREP_LOGISTIC_REGRESSION_TRANSFORM: String = "logistic-regression-data-prep"

    override def getParameters: scala.collection.immutable.Seq[Parameter] =  {
      val inputDatasetParameter = Parameter.newBuilder()
        .parameterName(INPUT_DATASET)
        .displayName("Dataset containing input records")
        .`type`(Parameter.TYPE_INPUT_COLLECTION)
        .required(true)
        .build()

      scala.collection.immutable.Seq(inputDatasetParameter)
    }

    /*
     * Next will implement our execute() function which will generate sample data
     * for logistic regression as a new data frame.
     */

    override def execute(context: DataFrameTransformContext): DataFrame = {

        /*
         * Prepare training data from a list of (label, features) tuples.
         */

      val spark = context.getSQLContext
      import spark.implicits._
      /*
       * Read in data generated from Logistic Regression Data Prep Transform
       */
      val training= context.getDataFrames.values.iterator.next

      val data = Seq((1.0, Vectors.dense(0.0, 1.1, 0.1)),
            (0.0, Vectors.dense(2.0, 1.0, -1.0)),
            (0.0, Vectors.dense(2.0, 1.3, 1.0)),
            (1.0, Vectors.dense(0.0, 1.2, -0.5))).toDF()

      data.count()
      data
    }



    /*
     * To complete our Transform we’ll give it a description, name, type ID, and version number
     */

    override def getDescription(): String = "Generate data set to write to Koverse so that it can be used " +
                "in the Logistic Regression Training Transform"

    override def getName(): String = "Logistic Regression Data Prep"

    override def getTypeId(): String = DATA_PREP_LOGISTIC_REGRESSION_TRANSFORM

    override def getVersion(): Version = new Version(0, 1, 0)

    override def supportsIncrementalProcessing: Boolean = false
}

