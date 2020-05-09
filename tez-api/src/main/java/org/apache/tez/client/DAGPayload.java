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
package org.apache.tez.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.records.DAGProtos;

/**
 * DAGPayload is simple base class in tez-api for making tez users able to submit a simple key-value
 * pair based payload with the dag. This payload is supposed to be lightweight and propagated to
 * plugins within TezAM, e.g. classes extending TaskCommunicator interface. Users of this api are
 * free to extend DAGPayload and create convenience methods, while tez takes care of
 * serializating/deserializating the inner payload. For details, see: TEZ-2672
 */
public class DAGPayload {

  private final Map<String, String> payload;

  public DAGPayload() {
    this.payload = new HashMap<String, String>();
  }

  public DAGPayload(DAGProtos.ConfigurationProto proto) {
    payload = new HashMap<String, String>();
    TezUtils.readMapFromPB(proto, payload);
  }

  public DAGPayload(Map<String, String> payload) {
    this.payload = payload;
  }

  public Map<String, String> getPayload() {
    return payload;
  }
}
