package com.koverse.examples.analytics;

import com.koverse.sdk.Version;
import com.koverse.sdk.data.Parameter;
import com.koverse.sdk.transform.java.DataFrameTransform;
import com.koverse.sdk.transform.java.DataFrameTransformContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Map;

import static com.koverse.com.google.common.collect.Lists.newArrayList;


public class SentimentAnalysis implements DataFrameTransform {

  /*
   * We’ll begin by defining a set of parameters that our Transform will use to request configuration information from a
   * user of the Koverse UI. In this case we’ll ask the user to tell our Transform which field in their data contains text
   * and which field contains a date:
   */

  public static final String TEXT_COL_PARAM = "textCol";
  public static final String DATE_COL_PARAM = "dateCol";
  public static final String ANALYZE_SENTIMENT_TRANSFORM = "analyze-sentiment";

  @Override
  public Iterable<Parameter> getParameters() {
    return newArrayList(
        Parameter.newBuilder()
            .displayName("Text field")
            .parameterName(TEXT_COL_PARAM)
            .required(Boolean.TRUE)
            .type(Parameter.TYPE_COLLECTION_FIELD)
            .build(),
        Parameter.newBuilder()
            .displayName("Date field")
            .parameterName(DATE_COL_PARAM)
            .required(Boolean.TRUE)
            .type(Parameter.TYPE_COLLECTION_FIELD)
            .build(),
        Parameter.newBuilder()
            .required(true)
            .type(Parameter.TYPE_INPUT_COLLECTION)
            .parameterName("inputDataset")
            .displayName("Dataset containing input records")
            .build());
  }

  /*
   * Next will implement our execute() function which will generate sentiment scores based on the text field and return a new
   * data frame with the new score field and original text and date fields. We’ll start by extracting the user specified names
   * for the text and date fields:
   *
   */

  @Override
  public Dataset<Row> execute(DataFrameTransformContext context) {
    String textCol = context.getParameters().get(TEXT_COL_PARAM);
    String dateCol = context.getParameters().get(DATE_COL_PARAM);


    /*
     * We’ll generate a sentiment score by using a word list published in the AFINN data set. This data set represented in the
     * AfinnData class as a static Java Map that we’ll use to lookup the sentiment score of each word and generate an average
     * sentiment for each message.
     *
     * To help us distribute this list in our Spark job, we’ll take advantage of Spark’s broadcast variables, which will
     * distribute our list once per executor so we don’t ship it on a per-task basis:
     */

    final Broadcast<Map<String, Integer>> broadcastWordList =
        context.getSparkContext().broadcast(AfinnData.getWordList(), scala.reflect.ClassTag$.MODULE$.apply(Map.class));

    /*
     * Since this is a data frame transform Spark expects us to use SQL functions or custom user-defined functions (UDFs).
     * We’ll write a UDF that references our word list to generate an overall score for each message based off of all the
     * words that appear in that message:
     */

    UDF1 sentimentUDF = new UDF1<String, Double>() {

      @Override
      public Double call(String text) throws Exception {

        Map<String, Integer> wordList = broadcastWordList.getValue();

        // compute average score from all words
        String[] words = text.toLowerCase().split("\\s+");
        Double score = 0.0;
        for (String word : words) {
          if (wordList.containsKey(word)) {
            score += wordList.get(word);
          }
        }

        score /= words.length;
        return score;
      }
    };

    /*
     * We have to register our UDF in order to use it to create a new column for our data frame:
     */

    context.getSQLContext().udf().register("sentimentUDF", sentimentUDF, DataTypes.DoubleType);

    /*
     * Now we’ll grab the data frame created by Koverse from a data set the user has specified. Then we’ll select only the
     * text column and date column from it (naming the text column “text” for consistency), drop any rows that are missing
     * a value for the date or text columns, and generate a new column consisting of sentiment scores using our UDF
     *
     * We return the resulting data frame and Koverse will store the information in that data frame as a new Data Set in Koverse.
     * It will index all the data in the Data Set and apply access protection to this Data Set. By default, the user that created
     * the resulting Data Set is the only user allowed to see the data within it until he or she decides to grant access to users
     * in other groups.
     */

    Dataset<Row> rowDataset = context.getDataFrames().values().iterator().next();

   return rowDataset
       .select(rowDataset.col(textCol).alias("text"), rowDataset.col(dateCol))
        .na().drop()
        .withColumn("score", functions.callUDF("sentimentUDF", rowDataset.col("text")));
  }

  /*
   * To complete our Transform we’ll give it a description, name, type ID, and version number
   */

  @Override
  public String getDescription() {
    return "Generate a sentiment score for each record containing text. "
        + "Also requires records to have a date field so changes in sentiment can be seen over time";
  }

  @Override
  public String getName() {
    return "Analyze Sentiment Over Time";
  }

  @Override
  public String getTypeId() {
    return ANALYZE_SENTIMENT_TRANSFORM;
  }

  @Override
  public Version getVersion() {
    return new Version(0, 1, 0);
  }

  @Override
  public boolean supportsIncrementalProcessing() {
    return false;
  }

}