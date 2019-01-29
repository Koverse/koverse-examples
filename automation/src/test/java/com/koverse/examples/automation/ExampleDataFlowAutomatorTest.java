package com.koverse.examples.automation;

import java.io.IOException;
import java.util.UUID;
import org.apache.thrift.TException;
import org.junit.Test;

public class ExampleDataFlowAutomatorTest {
  @Test
  public void runExampleDataFlowAutomatorWithChecks() throws IOException, TException, InterruptedException {
    String dataSetName = UUID.randomUUID().toString();

    String[] args = new String[]{};
    ExampleDataFlowAutomator.main(args);
  }
}