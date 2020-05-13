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
package org.apache.tez.protobuf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class for handling protobuf changes before/after Hadoop 3.3.
 */
public abstract class ProtobufHadoopShim {
  private static final Logger LOG = LoggerFactory.getLogger(ProtobufHadoopShim.class);
  private static boolean useHadoop33OrAbove = false;

  private static Method protoFromTokenMethod;

  static {
    try {
      protoFromTokenMethod = ProtobufHelper.class.getDeclaredMethod("protoFromToken");
      useHadoop33OrAbove = true;
    } catch (NoSuchMethodException e) {
      LOG.info(
          "Cannot find ProtobufHelper.protoFromToken method, falling back to TokenProto builder...");
    }
  }

  public static TokenProto protoFromToken(Token<?> tok) {
    try {
      return useHadoop33OrAbove ? (TokenProto) protoFromTokenMethod.invoke(null, tok)
        : buildProto(tok);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static TokenProto buildProto(Token<?> tok) {
    TokenProto.Builder builder = TokenProto.newBuilder();

    builder
        .setIdentifier(org.apache.tez.protobuf.ByteString.copyFrom(tok.getIdentifier()))
        .setPassword(org.apache.tez.protobuf.ByteString.copyFrom(tok.getPassword()))
        .setKind(tok.getKind().toString())
        .setService(tok.getService().toString());

    return builder.build();
  }
}
