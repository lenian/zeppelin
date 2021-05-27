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

package org.apache.zeppelin.flink;

import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.ZeppelinContext;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;

import java.util.List;

public interface AbstractFlinkScalaInterpreter {

  List<String> getUserJars();

  int getProgress(InterpreterContext context) throws InterpreterException;

  void cancel(InterpreterContext context) throws InterpreterException;

  void open();

  void close();

  InterpreterResult interpret(String st, InterpreterContext context);

  List<InterpreterCompletion> completion(String buf,
                                         int cursor,
                                         InterpreterContext interpreterContext);

  ExecutionEnvironment getExecutionEnvironment();

  StreamExecutionEnvironment getStreamExecutionEnvironment();

  TableEnvironment getStreamTableEnvironment(String planner);

  org.apache.flink.table.api.TableEnvironment getJavaBatchTableEnvironment(String planner);

  TableEnvironment getJavaStreamTableEnvironment(String planner);

  TableEnvironment getBatchTableEnvironment(String planner);

  JobManager getJobManager();

  ZeppelinContext getZeppelinContext();

  int getDefaultParallelism();

  int getDefaultSqlParallelism();

  /**
   * Workaround for issue of FLINK-16936.
   */
  void createPlannerAgain();

  ClassLoader getFlinkScalaShellLoader();

  Configuration getFlinkConfiguration();

  FlinkShims getFlinkShims();

  void setSavepointPathIfNecessary(InterpreterContext context);

  void setParallelismIfNecessary(InterpreterContext context);

  FlinkVersion getFlinkVersion();
}
