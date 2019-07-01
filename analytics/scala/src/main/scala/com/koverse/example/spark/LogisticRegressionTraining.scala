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

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame}
import com.koverse.sdk.Version
import com.koverse.sdk.data.Parameter
import com.koverse.sdk.transform.scala.{DataFrameTransform, DataFrameTransformContext}

class LogisticRegressionTraining extends DataFrameTransform {

    /*
     * We’ll begin by defining a set of parameters that our Transform will use to request configuration information from a
     * user of the Koverse UI. In this case we’ll ask the user to tell our Transform max iterations and regularization parameter.
     */

    final val MAX_ITERATIONS_PARAM = "maxIterations"
    final val REGULARIZATION_PARAM = "regularizationParameter"
    final val INPUT_DATASET = "inputDataSet"
    final val TRAIN_LOGISTIC_REGRESSION_TRANSFORM = "logistic-regression-train"

    def getParameters(): scala.collection.immutable.Seq[Parameter] = {

//        val iterationParameter = new Parameter(, "Max Iterations", )
//        val regularizationParameter = new Parameter(, "Regularization Variant", )
//        Seq(iterationParameter, regularizationParameter).asJava
         val iterationParameter = Parameter.newBuilder()
                                .displayName("Max Iterations")
                                .parameterName(MAX_ITERATIONS_PARAM)
                                .`type`(Parameter.TYPE_STRING)
                                .defaultValue("10")
                                .hint("Integer Number of Iterations")
                                .build()


        val regularizationParameter = Parameter.newBuilder()
                                .displayName("Regularization Variant")
                                .parameterName(REGULARIZATION_PARAM)
                                .`type`(Parameter.TYPE_STRING)
                                .defaultValue("0.01")
                                .hint("Method to improve Generalization (0.1 for one-tenth)")
                                .build()

        val inputDatasetParameter = Parameter.newBuilder()
          .parameterName(INPUT_DATASET)
          .displayName("Dataset containing input records")
          .`type`(Parameter.TYPE_INPUT_COLLECTION)
          .required(true)
          .build()

        scala.collection.immutable.Seq(inputDatasetParameter, iterationParameter, regularizationParameter)
    }

    /*
     * Next will implement our execute() function which will generate features and labels based on the max iterations,
     * sample data that is hard coded and regularizartion variant as a new data frame.
     */

  /**
    * Koverse calls this method to execute your transform.
    *
    * @param context The context of this spark execution
    * @return The resulting Dataset of this transform execution.
    *         It will be applied to the output collection.
    */
    override def execute(context: DataFrameTransformContext): DataFrame = {

        val iterations = context.getParameters.get(MAX_ITERATIONS_PARAM).get.toInt
        val regularization = java.lang.Double.parseDouble(context.getParameters.get(REGULARIZATION_PARAM).get)

        val spark = context.getSQLContext
        import spark.implicits._
        /*
         * Read in data generated from Logistic Regression Data Prep Transform
         */
        val training= context.getDataFrames.values.iterator.next


        /*
         * Create a LogisticRegression instance. This instance is an Estimator.
         * Print out the parameters, documentation, and any default values.
         */

         val lr = new LogisticRegression()
         println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

        /*
         * We may set parameters using setter methods.
         * Learn a LogisticRegression model. This uses the parameters stored in lr.
         * Since model1 is a Model (i.e., a Transformer produced by an Estimator), we can view the parameters
         * it used during fit(). This prints the parameter (name: value) pairs, where names are unique IDs for
         * this LogisticRegression instance.
         */

         lr.setMaxIter(iterations)
         .setRegParam(regularization)

         val model1 = lr.fit(training)

        println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)

        /*
         * We may alternatively specify parameters using a ParamMap,which supports several methods for specifying parameters.
         * Specify 1 Param. This overwrites the original maxIter. Specify multiple Params. One can also combine ParamMaps.
         * Change output column name. Now learn a new model using the paramMapCombined parameters. paramMapCombined
          * overrides all parameters set earlier via lr.set* methods.Prepare test data.
         */

        val paramMap = ParamMap(lr.maxIter -> 20)
        .put(lr.maxIter, 30)
        .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

        val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
        val paramMapCombined = paramMap ++ paramMap2


        val model2 = lr.fit(training, paramMapCombined)
        println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

        val test: DataFrame = Seq((1.0, Vectors.dense(-1.0, 1.5, 1.3)),
                     (0.0, Vectors.dense(3.0, 2.0, -0.1)),
                      (1.0, Vectors.dense(0.0, 2.2, -1.5))).toDF("label", "features")


        /*
         *  Make predictions on test data using the Transformer.transform() method. LogisticRegression transform will
         *  only use the 'features' column. Note that model2.transform() outputs a 'myProbability' column instead of the
         *  usual 'probability' column since we renamed the lr.probabilityCol parameter previously.
         */

       model2.transform(test)
                .select("features", "label", "myProbability", "prediction")

    }


    /*
     * To complete our Transform we’ll give it a description, name, type ID, and version number
     */

    override def getDescription(): String = "Generate features and labels for the result of logistic regression training"


    override def getName(): String = "Logistic Regression Training"


    override def getTypeId(): String = TRAIN_LOGISTIC_REGRESSION_TRANSFORM


    override def getVersion(): Version = new Version(0, 1, 0)

    override def supportsIncrementalProcessing(): Boolean = true

}
