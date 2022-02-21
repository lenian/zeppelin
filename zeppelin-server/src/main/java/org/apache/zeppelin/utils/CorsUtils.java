/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.utils;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CorsUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(CorsUtils.class);
  public static final String HEADER_ORIGIN = "Origin";


  public static Boolean isValidOrigin(String sourceHost, ZeppelinConfiguration conf)
      throws UnknownHostException, URISyntaxException {

    String sourceUriHost = "";

    if (sourceHost != null && !sourceHost.isEmpty()) {
      sourceUriHost = new URI(sourceHost).getHost();
      sourceUriHost = (sourceUriHost == null) ? "" : sourceUriHost.toLowerCase();
    }

    sourceUriHost = sourceUriHost.toLowerCase();
    String currentHost = InetAddress.getLocalHost().getHostName().toLowerCase();

    LOGGER.info("Allowed origins:{}", StringUtils.join(conf.getAllowedOrigins()));
    LOGGER.info("currentHost:{}", currentHost);
    LOGGER.info("sourceHost:{}", sourceHost);
    LOGGER.info("sourceUriHost:{}", sourceUriHost);

    return conf.getAllowedOrigins().contains("*")
        || currentHost.equals(sourceUriHost)
        || "localhost".equals(sourceUriHost)
        || conf.getAllowedOrigins().contains(sourceHost);
  }
}
