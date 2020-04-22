/**
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

package org.apache.tez.dag.history.events;

import java.io.IOException;

import org.apache.tez.protobuf.CodedInputStream;
import org.apache.tez.protobuf.CodedOutputStream;
import org.apache.tez.dag.app.dag.VertexState;
import org.apache.tez.dag.history.HistoryEvent;
import org.apache.tez.dag.history.HistoryEventType;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.recovery.records.RecoveryProtos.VertexStartedProto;

public class VertexStartedEvent implements HistoryEvent {

  private TezVertexID vertexID;
  private long startRequestedTime;
  private long startTime;

  public VertexStartedEvent() {
  }

  public VertexStartedEvent(TezVertexID vertexId,
      long startRequestedTime, long startTime) {
    this.vertexID = vertexId;
    this.startRequestedTime = startRequestedTime;
    this.startTime = startTime;
  }

  @Override
  public HistoryEventType getEventType() {
    return HistoryEventType.VERTEX_STARTED;
  }

  @Override
  public boolean isRecoveryEvent() {
    return true;
  }

  @Override
  public boolean isHistoryEvent() {
    return true;
  }

  public VertexStartedProto toProto() {
    return VertexStartedProto.newBuilder()
        .setVertexId(vertexID.toString())
        .setStartRequestedTime(startRequestedTime)
        .setStartTime(startTime)
        .build();
  }

  public void fromProto(VertexStartedProto proto) {
    this.vertexID = TezVertexID.fromString(proto.getVertexId());
    this.startRequestedTime = proto.getStartRequestedTime();
    this.startTime = proto.getStartTime();
  }

  @Override
  public void toProtoStream(CodedOutputStream outputStream) throws IOException {
    outputStream.writeMessageNoTag(toProto());
  }

  @Override
  public void fromProtoStream(CodedInputStream inputStream) throws IOException {
    VertexStartedProto proto = inputStream.readMessage(VertexStartedProto.PARSER, null);
    if (proto == null) {
      throw new IOException("No data found in stream");
    }
    fromProto(proto);
  }

  @Override
  public String toString() {
    return "vertexId=" + vertexID
        + ", startRequestedTime=" + startRequestedTime
        + ", startedTime=" + startTime;
  }

  public TezVertexID getVertexID() {
    return this.vertexID;
  }

  public long getStartRequestedTime() {
    return startRequestedTime;
  }

  public long getStartTime() {
    return startTime;
  }

  public VertexState getVertexState() {
    return VertexState.RUNNING;
  }
}
