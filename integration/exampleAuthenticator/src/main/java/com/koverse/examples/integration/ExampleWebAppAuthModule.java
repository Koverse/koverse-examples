package com.koverse.examples.integration;

import com.koverse.sdk.security.webapp.AbstractWebAppAuthModule;
import com.koverse.sdk.security.webapp.HttpServletRequestAuthenticator;
import com.koverse.sdk.security.webapp.WebAppAuthorizer;
import com.koverse.sdk.security.webapp.WebAppParameterAuthenticator;

import com.google.inject.multibindings.Multibinder;

public class ExampleWebAppAuthModule extends AbstractWebAppAuthModule {

  @Override
  protected void configure(
      Multibinder<WebAppAuthorizer> authorizersBinder,
      Multibinder<HttpServletRequestAuthenticator> servletRequestAuthenticatorsBinder,
      Multibinder<WebAppParameterAuthenticator> parameterAuthenticatorsBinder) {

    parameterAuthenticatorsBinder.addBinding().to(ExampleWebAppParameterAuthenticator.class);
  }
}
