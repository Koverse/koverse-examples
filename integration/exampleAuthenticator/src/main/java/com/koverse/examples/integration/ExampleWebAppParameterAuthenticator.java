package com.koverse.examples.integration;

import com.koverse.com.google.common.base.Optional;
import com.koverse.sdk.security.webapp.WebAppParameterAuthenticator;
import com.koverse.sdk.security.webapp.WebAppParameterAuthenticatorDescription;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple example of a custom WebAppParameterAuthenticator.
 * This class illustrates adding custom parameters.
 * In this example we assume the user has a second-factor authentication device that provides sequence numbers.
 */
public class ExampleWebAppParameterAuthenticator implements WebAppParameterAuthenticator {

  public static final String USERNAME_PARAM = "usernameParam";
  public static final String PASSWORD_PARAM = "passwordParam";
  public static final String SEQ_NUM_PARAM = "sequenceNumberParam";

  private static final Map<String, String> dummyUserDb = new HashMap<>();

  static {
    // setup a dummy database of usernames and passwords
    // a real authenticator would talk to an external authentication service
    dummyUserDb.put("test user", "test passwd");
  }

  @Override
  public WebAppParameterAuthenticatorDescription getDescription() {
    return new ExampleWebAppParameterAuthenticatorDescription();
  }

  @Override
  public Optional<String> authenticate(Map<String, String> parameterValues) {

    if (parameterValues.containsKey(USERNAME_PARAM)
        && parameterValues.containsKey(PASSWORD_PARAM)
        && parameterValues.containsKey(SEQ_NUM_PARAM)) {

      String username = parameterValues.get(USERNAME_PARAM);

      String pass = parameterValues.get(PASSWORD_PARAM);

      String seq = parameterValues.get(SEQ_NUM_PARAM);

      if (!username.isEmpty() && !pass.isEmpty() && !seq.isEmpty()) {

        // first authenticate the user and password
        if (dummyUserDb.containsKey(username)) {

          String checkPass = dummyUserDb.get(username);

          if (pass.equals(checkPass)) {

            // now verify the second factor
            try {

              int sequenceNum = Integer.parseInt(seq);

              int checkNumber = DummySecondFactorGenerator.generateSequenceNumber();

              if (sequenceNum == checkNumber) {
                // success!
                return Optional.of(username);
              }
            } catch (NumberFormatException nfe) {
              // we just fail to authenticate
            }
          }
        }
      }
    }

    return Optional.absent();
  }
}
