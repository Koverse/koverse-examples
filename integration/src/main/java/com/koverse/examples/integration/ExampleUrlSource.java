/*
 * Copyright 2018 Koverse, Inc.
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

package com.koverse.examples.integration;


import static com.koverse.com.google.common.base.Predicates.notNull;
import static com.koverse.com.google.common.collect.Lists.newArrayList;

import com.koverse.com.google.common.base.Function;
import com.koverse.com.google.common.base.Splitter;
import com.koverse.com.google.common.collect.FluentIterable;
import com.koverse.sdk.data.Parameter;
import com.koverse.sdk.source.AbstractFileBasedSource;
import com.koverse.sdk.source.ImportSourcePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.Date;
import java.util.List;


public class ExampleUrlSource extends AbstractFileBasedSource {

  public static final String URL_SOURCE_TYPE_ID = "url-source";
  public static final String URLS_PARAMETER = "koverse_input_urls";

  private static final Logger logger = LoggerFactory.getLogger(ExampleUrlSource.class);

  private FluentIterable<String> urls;


  @Override
  public void configureFileBasedSource() throws IOException {

    urls =
        FluentIterable.from(Splitter.on(',').trimResults().omitEmptyStrings()
            .split(getContext().getParameterValues().get(URLS_PARAMETER)));
  }

  @Override
  public List<Parameter> getParameters() {
    return Collections.emptyList();
  }

  @Override
  public List<Parameter> getFileBasedFlowParameters() {
    return newArrayList(
        Parameter.newBuilder()
            .parameterName(URLS_PARAMETER)
            .displayName("Comma-separated list of URLs")
            .type(Parameter.TYPE_TEXTAREA)
            .parameterGroup("Target")
            .required(Boolean.TRUE)
            .placeholder("http://www.example.com/path/file.csv, http://www.example.com/path//to/other/file.csv")
            .position(1)
            .build());
  }

  @Override
  public String getName() {
    return "Example URL Source";
  }

  @Override
  public String getVersion() {
    return "1.0.0";
  }

  @Override
  public String getSourceTypeId() {
    return URL_SOURCE_TYPE_ID;
  }

  @Override
  public String getDescription() {
    return "Retrieve the contents of one or more URLs. Koverse will connect to the URL and download a single file, "
        + "if one is specified, or else can optionally try to download all of the files listed in the response from the URL.";
  }

  @Override
  public InputStream streamForItem(String item) throws IOException {
    final URL url = createUrl(item);
    final URLConnection conn = url.openConnection();

    return conn.getInputStream();
  }


  /**
   * If a URL has the classpath protocol, use our custom URL Stream Handlers, otherwise, just use the default.
   *
   * @param spec The URL spec
   * @return The URL
   */
  private URL createUrl(String spec) throws MalformedURLException {
    return new URL(spec);
  }


  @Override
  public final Boolean isContinuous() {
    return false;
  }

  private Function<String, ImportSourcePath> transformStringToImportSourcePath() {
    return new Function<String, ImportSourcePath>() {
      @Override
      public ImportSourcePath apply(String urlString) {
        try {
          final URL urlCheck = createUrl(urlString);
          final URLConnection connCheck = urlCheck.openConnection();
          final long lastModifiedTime = connCheck.getLastModified();
          final Date lastModifiedDate;

          if (lastModifiedTime == 0) {
            lastModifiedDate = new Date();
          } else {
            lastModifiedDate = new Date(lastModifiedTime);
          }

          return new ImportSourcePath.Builder().path(urlString).lastModifiedDate(lastModifiedDate)
              .fileSize(connCheck.getContentLengthLong()).build();
        } catch (MalformedURLException ex) {
          logger.warn("Malformed URL: {}. Skipping", urlString, ex);
          return null;
        } catch (IOException ex) {
          logger.warn("Could not open URL: {}. Skipping", urlString, ex);
          return null;
        }
      }
    };
  }

  @Override
  public Iterable<ImportSourcePath> enumerateUnfilteredList() throws Exception {

    return urls.transform(transformStringToImportSourcePath()).filter(notNull()).toList();
  }

  @Override
  public boolean supportsInputStreamReset() {
    return false;
  }

}
