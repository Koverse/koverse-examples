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

package com.koverse.examples.analytics;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import com.koverse.sdk.Version;
import com.koverse.sdk.data.Parameter;
import com.koverse.sdk.transform.java.DataFrameTransform;
import com.koverse.sdk.transform.java.DataFrameTransformContext;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;

public class JavaTokenizerDataFrameTransform implements DataFrameTransform {

  private static final String TEXT_FIELD_PARAM = "textFieldParam";

  @Override
  public Dataset<Row> execute(DataFrameTransformContext sparkTransformContext) {

    final String textField = sparkTransformContext.getParameters().get(TEXT_FIELD_PARAM);
    SparkSession spark = sparkTransformContext.getSparkSession();

    Dataset<Row> dataset =
        sparkTransformContext.getDataFrames().values().iterator().next();

    RegexTokenizer regexTokenizer = new RegexTokenizer()
        .setInputCol("article")
        .setOutputCol("words")
        .setPattern("\\W")
        .setToLowercase(true);

    spark.udf().register(
        "countTokens", (WrappedArray<?> words) -> words.size(), DataTypes.IntegerType);

    Dataset<Row> regexTokenized = regexTokenizer.transform(dataset);

    return regexTokenized.select("article", "words")
        .withColumn("tokens", callUDF("countTokens", col("words")));
  }

  @Override
  public Iterable<Parameter> getParameters() {
    ArrayList<Parameter> params = new ArrayList<>();
    params.add(Parameter.newBuilder()
        .parameterName(TEXT_FIELD_PARAM)
        .displayName("Text field")
        .hint("Field containing text.")
        .type(Parameter.TYPE_COLLECTION_FIELD)
        .defaultValue("")
        .required(true)
        .build());

    params.add(Parameter.newBuilder()
        .required(true)
        .type(Parameter.TYPE_INPUT_COLLECTION)
        .parameterName("inputDataset")
        .displayName("Dataset containing input records")
        .build());

    return params;
  }

  @Override
  public String getName() {
    return "Spark Tokenizer DataFrame Tranform";
  }

  @Override
  public String getTypeId() {
    return "java-tokenizer-dataframe-transform";
  }

  @Override
  public Version getVersion() {
    return new Version(0, 1, 0);
  }

  @Override
  public boolean supportsIncrementalProcessing() {
    return false;
  }


  @Override
  public String getDescription() {
    return "Basic tokenizer for Dataframe " + "Uses Apache Spark.";
  }

}
