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

import com.koverse.sdk.Version;
import com.koverse.sdk.data.Parameter;
import com.koverse.sdk.data.SimpleRecord;
import com.koverse.sdk.transform.java.RDDTransform;
import com.koverse.sdk.transform.java.RDDTransformContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


import java.util.ArrayList;
import java.util.Arrays;

public class JavaWordCountTransform implements RDDTransform {

  private static final String TEXT_FIELD_PARAM = "textFieldParam";

  @Override
  public JavaRDD<SimpleRecord> execute(RDDTransformContext sparkTransformContext) {

    final String textField = sparkTransformContext.getParameters().get(TEXT_FIELD_PARAM);

    JavaRDD<SimpleRecord> rdd =
        sparkTransformContext.getJavaRDDs().values().iterator().next();

    JavaRDD<String> words = rdd
        .filter((Function<SimpleRecord, Boolean>) t1 -> t1.containsKey(textField))
        .flatMap((FlatMapFunction<SimpleRecord, String>) t ->
            Arrays.asList(t.get(textField).toString().toLowerCase().split("\\s+")).iterator());

    // calculate counts
    return words.mapToPair((PairFunction<String, String, Integer>) t -> new Tuple2<>(t, 1))
        .reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b)
        .map((Function<Tuple2<String, Integer>, SimpleRecord>) t1 -> {
          SimpleRecord r = new SimpleRecord();
          r.put("word", t1._1);
          r.put("count", t1._2);
          return r;
        });
  }

  @Override
  public Iterable<Parameter> getParameters() {
    ArrayList<Parameter> params = new ArrayList<Parameter>();
    params.add(Parameter.newBuilder()
        .parameterName(TEXT_FIELD_PARAM)
        .displayName("Text field")
        .hint("Field containing text.")
        .type(Parameter.TYPE_COLLECTION_FIELD)
        .defaultValue("")
        .required(true)
        .build());
    return params;
  }

  @Override
  public String getName() {
    return "Spark Word Count";
  }

  @Override
  public String getTypeId() {
    return "spark-word-count";
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
    return "Count all words in the text field of the input data sets and write the counts for each word to the output data set. "
        + "Uses Apache Spark.";
  }

}
