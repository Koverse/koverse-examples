package com.koverse.examples.integration;

import com.koverse.client.thrift.Client;
import com.koverse.thrift.dataflow.TSource;
import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.TException;

public class ExampleWikipediaSource {

  final static String sourceName = "Example wikipedia source";

  public static TSource getSource(Client client) throws TException {
    // create source
    TSource tSource = new TSource();
    tSource.setName(sourceName);

    // other sources possible but this example is pulling data from wikipedia pages
    tSource.setTypeId("wikipedia-pages-source");

    // wikipedia pages require additional parameters
    Map<String,String> sourceParams = new HashMap<>();
    sourceParams.put("pageTitleListParam", "Cat Dog");
    tSource.setParameters(sourceParams);

    return client.createSourceInstance(tSource);
  }
}
