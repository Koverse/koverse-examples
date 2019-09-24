package com.koverse.examples.integration;

import com.koverse.dto.json.JsonParameter;
import com.koverse.sdk.data.Parameter;
import com.koverse.sdk.security.webapp.WebAppParameterAuthenticator;
import com.koverse.sdk.security.webapp.WebAppParameterAuthenticatorDescription;

import java.util.ArrayList;
import java.util.List;

public class ExampleWebAppParameterAuthenticatorDescription implements WebAppParameterAuthenticatorDescription {

  @Override
  public Class<? extends WebAppParameterAuthenticator> getAuthenticatorClass() {
    return ExampleWebAppParameterAuthenticator.class;
  }

  @Override
  public String getDisplayName() {
    return "Example Web App Parameter Authenticator";
  }

  @Override
  public String getTypeId() {
    return "example-web-app-parameter-authenticator";
  }

  @Override
  public List<JsonParameter> getAuthenticationParameters() {

    ArrayList<JsonParameter> params = new ArrayList<>();

    JsonParameter usernameParam = new JsonParameter();
    usernameParam.setDisplayName("Username");
    usernameParam.setParameterName(ExampleWebAppParameterAuthenticator.USERNAME_PARAM);
    usernameParam.setRequired(true);
    usernameParam.setType(Parameter.TYPE_STRING);

    params.add(usernameParam);

    JsonParameter passwordParam = new JsonParameter();
    passwordParam.setDisplayName("Password");
    passwordParam.setParameterName(ExampleWebAppParameterAuthenticator.PASSWORD_PARAM);
    passwordParam.setRequired(true);
    passwordParam.setType(Parameter.TYPE_STRING);

    params.add(passwordParam);

    JsonParameter seqNumParam = new JsonParameter();
    seqNumParam.setDisplayName("Sequence Number");
    seqNumParam.setParameterName(ExampleWebAppParameterAuthenticator.SEQ_NUM_PARAM);
    seqNumParam.setRequired(true);
    seqNumParam.setType(Parameter.TYPE_INTEGER);

    params.add(seqNumParam);

    return params;
  }
};

