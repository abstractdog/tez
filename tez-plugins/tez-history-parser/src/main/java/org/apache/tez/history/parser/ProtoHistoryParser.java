/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.history.parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.tez.common.Preconditions;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.history.logging.proto.HistoryEventProtoJsonConversion;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;
import org.apache.tez.history.parser.datamodel.DagInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public class ProtoHistoryParser extends SimpleHistoryParser {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoHistoryParser.class);
  private List<File> protoFiles;

  public ProtoHistoryParser(List<File> files) {
    super(files);
    this.protoFiles = files;
  }

  /**
   * Get in-memory representation of DagInfo
   *
   * @return DagInfo
   * @throws TezException
   */
  public DagInfo getDAGData(String dagId) throws TezException {
    try {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(dagId), "Please provide valid dagId");
      dagId = dagId.trim();
      parseContents(protoFiles, dagId);
      linkParsedContents();
      return dagInfo;
    } catch (IOException e) {
      LOG.error("Error in reading DAG ", e);
      throw new TezException(e);
    } catch (JSONException e) {
      LOG.error("Error in parsing DAG ", e);
      throw new TezException(e);
    }
  }

  protected void parseContents(List<File> protoFiles, String dagId)
      throws JSONException, FileNotFoundException, TezException, IOException {
    final TezConfiguration conf = new TezConfiguration();

    Iterator<File> fileIt = protoFiles.iterator();

    JSONObjectSource source = new JSONObjectSource() {
      private HistoryEventProto message = null;
      ProtoMessageReader reader = new ProtoMessageReader(conf, new Path(fileIt.next().getPath()),
          HistoryEventProto.PARSER);

      @Override
      public JSONObject next() throws JSONException {
        return HistoryEventProtoJsonConversion.convertToJson(message);
      }

      @Override
      public boolean hasNext() throws IOException {
        try {
          message = (HistoryEventProto) reader.readEvent();
          return message != null;
        } catch (java.io.EOFException e) {
          reader.close();

          if (!fileIt.hasNext()) {
            return false;
          } else {
            reader = new ProtoMessageReader(conf, new Path(fileIt.next().getPath()),
                HistoryEventProto.PARSER);
            try {
              message = (HistoryEventProto) reader.readEvent();
              return message != null;
            } catch (java.io.EOFException e2) {
              return false;
            }
          }
        }
      }

      @Override
      public void close() {
        try {
          reader.close();
        } catch (IOException e) {
          LOG.warn("error while closing ProtoMessageReader", e);
        }
      }
    };

    Map<String, JSONObject> vertexJsonMap = Maps.newHashMap();
    Map<String, JSONObject> taskJsonMap = Maps.newHashMap();
    Map<String, JSONObject> attemptJsonMap = Maps.newHashMap();

    readEventsFromSource(dagId, source, vertexJsonMap, taskJsonMap, attemptJsonMap);
    postProcessMaps(vertexJsonMap, taskJsonMap, attemptJsonMap);
  }
}