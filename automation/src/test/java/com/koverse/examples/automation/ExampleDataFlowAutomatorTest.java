package com.koverse.examples.automation;

import com.koverse.thrift.security.TAuthorizationException;

import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;

public class ExampleDataFlowAutomatorTest {

  @Test
  public void runExampleDataFlowAutomator() throws TException, IOException, InterruptedException {
    try {
      String[] args = new String[]{};
      ExampleDataFlowAutomator.main(args);
    } catch (TAuthorizationException auth) {
      System.out.println("\nERROR: Make sure you set up the credentials in src/test/resources/client.properties before running this test.\n");
    }
  }

}