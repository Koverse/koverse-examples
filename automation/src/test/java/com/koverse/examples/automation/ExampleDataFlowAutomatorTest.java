package com.koverse.examples.automation;

import java.io.IOException;
import org.apache.thrift.TException;
import org.junit.Test;

public class ExampleDataFlowAutomatorTest {

  @Test
  public void runExampleDataFlowAutomator() throws TException, IOException, InterruptedException {
    String[] args = new String[]{};
    ExampleDataFlowAutomator.main(args);
  }

}