/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.flink.sql;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.experimental.CollectSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypes;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.net.InetAddress;

/**
 * Table sink for collecting the results locally using sockets.
 */
public class CollectStreamTableSink implements RetractStreamTableSink<Row> {

  private final InetAddress targetAddress;
  private final int targetPort;
  private final TypeSerializer<Tuple2<Boolean, Row>> serializer;

  private String[] fieldNames;
  private DataType[] fieldTypes;

  public CollectStreamTableSink(InetAddress targetAddress,
                                int targetPort,
                                TypeSerializer<Tuple2<Boolean, Row>> serializer) {
    this.targetAddress = targetAddress;
    this.targetPort = targetPort;
    this.serializer = serializer;
  }

  @Override
  public String[] getFieldNames() {
    return fieldNames;
  }

  @Override
  public DataType[] getFieldTypes() {
    return fieldTypes;
  }

  @Override
  public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, DataType[] fieldTypes) {
    final CollectStreamTableSink copy =
            new CollectStreamTableSink(targetAddress, targetPort, serializer);
    copy.fieldNames = fieldNames;
    copy.fieldTypes = fieldTypes;
    return copy;
  }

  @Override
  public DataType getRecordType() {
    return DataTypes.createRowType(fieldTypes, fieldNames);
  }

  @Override
  public DataStreamSink<Tuple2<Boolean, Row>> emitDataStream(
          DataStream<Tuple2<Boolean, Row>> stream) {
    // add sink
    return stream
            .addSink(new CollectSink<>(targetAddress, targetPort, serializer))
            .name("SQL Client Stream Collect Sink")
            .setParallelism(1);
  }

  @Override
  public DataType getOutputType() {
    return DataTypes.createTupleType(DataTypes.BOOLEAN, getRecordType());
  }
}
