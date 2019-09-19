package com.koverse.examples.integration;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.koverse.com.google.common.base.Optional;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ExampleWebAppParameterAuthenticatorTest {

  @Test
  public void testSuccess() {

    ExampleWebAppParameterAuthenticator authenticator = new ExampleWebAppParameterAuthenticator();

    Map<String, String> params = new HashMap<>();

    params.put(ExampleWebAppParameterAuthenticator.USERNAME_PARAM, "test user");
    params.put(ExampleWebAppParameterAuthenticator.PASSWORD_PARAM, "test passwd");

    // simulate a user who as a second authentication factor device
    int seqNumber = DummySecondFactorGenerator.generateSequenceNumber();
    params.put(ExampleWebAppParameterAuthenticator.SEQ_NUM_PARAM, Integer.toString(seqNumber));

    Optional<String> result = authenticator.authenticate(params);

    assertTrue(result.isPresent());
    assertEquals("test user", result.get());
  }

  @Test
  public void testFailOnWrongUsername() {

    ExampleWebAppParameterAuthenticator authenticator = new ExampleWebAppParameterAuthenticator();

    Map<String, String> params = new HashMap<>();

    params.put(ExampleWebAppParameterAuthenticator.USERNAME_PARAM, "wrong");
    params.put(ExampleWebAppParameterAuthenticator.PASSWORD_PARAM, "test passwd");

    // simulate a user who as a second authentication factor device
    int seqNumber = DummySecondFactorGenerator.generateSequenceNumber();
    params.put(ExampleWebAppParameterAuthenticator.SEQ_NUM_PARAM, Integer.toString(seqNumber));

    Optional<String> result = authenticator.authenticate(params);

    assertFalse(result.isPresent());
  }

  @Test
  public void testFailOnWrongPassword() {

    ExampleWebAppParameterAuthenticator authenticator = new ExampleWebAppParameterAuthenticator();

    Map<String, String> params = new HashMap<>();

    params.put(ExampleWebAppParameterAuthenticator.USERNAME_PARAM, "test user");
    params.put(ExampleWebAppParameterAuthenticator.PASSWORD_PARAM, "wrong");

    // simulate a user who as a second authentication factor device
    int seqNumber = DummySecondFactorGenerator.generateSequenceNumber();
    params.put(ExampleWebAppParameterAuthenticator.SEQ_NUM_PARAM, Integer.toString(seqNumber));

    Optional<String> result = authenticator.authenticate(params);

    assertFalse(result.isPresent());
  }

  @Test
  public void testFailOnWrongSequence() {

    ExampleWebAppParameterAuthenticator authenticator = new ExampleWebAppParameterAuthenticator();

    Map<String, String> params = new HashMap<>();

    params.put(ExampleWebAppParameterAuthenticator.USERNAME_PARAM, "test user");
    params.put(ExampleWebAppParameterAuthenticator.PASSWORD_PARAM, "test passwd");

    // simulate a user who as a second authentication factor device
    int seqNumber = DummySecondFactorGenerator.generateSequenceNumber() + 1234;
    params.put(ExampleWebAppParameterAuthenticator.SEQ_NUM_PARAM, Integer.toString(seqNumber));

    Optional<String> result = authenticator.authenticate(params);

    assertFalse(result.isPresent());
  }
}
