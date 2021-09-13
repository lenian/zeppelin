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

package org.apache.zeppelin.interpreter.launcher;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterOption;
import org.apache.zeppelin.interpreter.InterpreterRunner;
import org.apache.zeppelin.interpreter.integration.DownloadUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class FlinkInterpreterLauncherTest {

  private String flinkHome;

  public FlinkInterpreterLauncherTest() {
    flinkHome = DownloadUtils.downloadFlink("1.13.2", "2.11");
  }

  @Test
  public void testLocalMode() throws IOException {
    ZeppelinConfiguration zConf = ZeppelinConfiguration.create();
    FlinkInterpreterLauncher launcher = new FlinkInterpreterLauncher(zConf, null);
    Properties intpProperties = new Properties();
    intpProperties.put("flink.execution.mode", "local");
    InterpreterLaunchContext launchContext = new InterpreterLaunchContext(intpProperties,
            new InterpreterOption(),
            new InterpreterRunner(),
            "user1",
            "group1",
            "flink",
            "flink",
            "flink",
            1234,
            "localhost");
    try {
      Map<String, String> envs = launcher.buildEnvFromProperties(launchContext);
      fail("Should fail because no FLINK_HOME is specified");
    } catch (Exception e) {
      assertEquals("FLINK_HOME is not specified", e.getMessage());
    }

    intpProperties.put("FLINK_HOME", flinkHome);
    Map<String, String> envs = launcher.buildEnvFromProperties(launchContext);
    assertEquals(flinkHome, envs.get("FLINK_HOME"));
    assertEquals(flinkHome + "/conf", envs.get("FLINK_CONF_DIR"));
    assertEquals(flinkHome + "/lib", envs.get("FLINK_LIB_DIR"));
    assertEquals(flinkHome + "/plugins", envs.get("FLINK_PLUGINS_DIR"));
    assertNotNull(envs.get("FLINK_APP_JAR"));
    assertFalse(envs.containsKey("ZEPPELIN_FLINK_YARN_APPLICATION"));
    assertFalse(envs.containsKey("ZEPPELIN_FLINK_YARN_APPLICATION_CONF"));
  }

  @Test
  public void testYarnMode() throws IOException {
    ZeppelinConfiguration zConf = ZeppelinConfiguration.create();
    FlinkInterpreterLauncher launcher = new FlinkInterpreterLauncher(zConf, null);
    Properties intpProperties = new Properties();
    intpProperties.put("flink.execution.mode", "yarn");
    intpProperties.put("FLINK_HOME", flinkHome);
    InterpreterLaunchContext launchContext = new InterpreterLaunchContext(intpProperties,
            new InterpreterOption(),
            new InterpreterRunner(),
            "user1",
            "group1",
            "flink",
            "flink",
            "flink",
            1234,
            "localhost");

    Map<String, String> envs = launcher.buildEnvFromProperties(launchContext);
    assertEquals(flinkHome, envs.get("FLINK_HOME"));
    assertEquals(flinkHome + "/conf", envs.get("FLINK_CONF_DIR"));
    assertEquals(flinkHome + "/lib", envs.get("FLINK_LIB_DIR"));
    assertEquals(flinkHome + "/plugins", envs.get("FLINK_PLUGINS_DIR"));
    assertNotNull(envs.get("FLINK_APP_JAR"));
    assertFalse(envs.containsKey("ZEPPELIN_FLINK_YARN_APPLICATION"));
    assertFalse(envs.containsKey("ZEPPELIN_FLINK_YARN_APPLICATION_CONF"));
  }

  @Test
  public void testYarnApplicationMode() throws IOException {
    ZeppelinConfiguration zConf = ZeppelinConfiguration.create();
    FlinkInterpreterLauncher launcher = new FlinkInterpreterLauncher(zConf, null);
    Properties intpProperties = new Properties();
    intpProperties.put("flink.execution.mode", "yarn-application");
    intpProperties.put("FLINK_HOME", flinkHome);
    InterpreterLaunchContext launchContext = new InterpreterLaunchContext(intpProperties,
            new InterpreterOption(),
            new InterpreterRunner(),
            "user1",
            "group1",
            "flink",
            "flink",
            "flink",
            1234,
            "localhost");

    Map<String, String> envs = launcher.buildEnvFromProperties(launchContext);
    assertEquals(flinkHome, envs.get("FLINK_HOME"));
    assertEquals(flinkHome + "/conf", envs.get("FLINK_CONF_DIR"));
    assertEquals(flinkHome + "/lib", envs.get("FLINK_LIB_DIR"));
    assertEquals(flinkHome + "/plugins", envs.get("FLINK_PLUGINS_DIR"));
    assertNotNull(envs.get("FLINK_APP_JAR"));
    assertEquals("true", envs.get("ZEPPELIN_FLINK_YARN_APPLICATION"));
    assertEquals("", envs.get("ZEPPELIN_FLINK_YARN_APPLICATION_CONF"));
  }
}
