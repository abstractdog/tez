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

package org.apache.tez.client.registry;

import java.io.IOException;
import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.zookeeper.ZkConfig;

/**
 * Represents an instance of an AM (DAGClientServer) in the AM registry
 */
@InterfaceAudience.Public
public class AMRecord {
  private ApplicationId appId;
  private String hostName;
  private String hostIp;
  private int port;
  private String externalId;
  private String computeName;
  private Token clientToAMToken;
  private final static String APP_ID_RECORD_KEY = "appId";
  private final static String HOST_NAME_RECORD_KEY = "hostName";
  private final static String HOST_IP_RECORD_KEY = "hostIp";
  private final static String PORT_RECORD_KEY = "port";
  private final static String EXTERNAL_ID_KEY = "externalId";
  private final static String COMPUTE_GROUP_NAME_KEY = "computeName";
  private final static String CLIENT_TO_AM_TOKEN_KEY = "clientToAMToken";

  public AMRecord(ApplicationId appId, String hostName, String hostIp, int port, final String externalId,
                  String computeName) {
    this(appId, hostName, hostIp, port, externalId, computeName, null);
  }

  public AMRecord(ApplicationId appId, String hostName, String hostIp, int port, final String externalId,
    String computeName, Token clientToAMToken) {
    Preconditions.checkNotNull(appId);
    Preconditions.checkNotNull(hostName);
    this.appId = appId;
    this.hostName = hostName;
    this.hostIp = hostIp;
    this.port = port;
    //externalId is optional, if not provided, convert to empty string
    this.externalId = (externalId == null) ? "" : externalId;
    this.computeName = (computeName == null) ? ZkConfig.DEFAULT_COMPUTE_GROUP_NAME : computeName;
    this.clientToAMToken = clientToAMToken;
  }

  public AMRecord(AMRecord other) {
    Preconditions.checkNotNull(other);
    this.appId = other.getApplicationId();
    this.hostName = other.getHost();
    this.hostIp = other.getHostIp();
    this.port = other.getPort();
    this.externalId = other.getExternalId();
    this.computeName = other.getComputeName();
    this.clientToAMToken = other.getClientToAMToken();
  }

  public AMRecord(ServiceRecord serviceRecord) {
    String serviceAppId = serviceRecord.get(APP_ID_RECORD_KEY);
    Preconditions.checkNotNull(serviceAppId);
    this.appId = ApplicationId.fromString(serviceAppId);
    String serviceHost = serviceRecord.get(HOST_NAME_RECORD_KEY);
    Preconditions.checkNotNull(serviceHost);
    this.hostName = serviceHost;
    String serviceHostIp = serviceRecord.get(HOST_IP_RECORD_KEY);
    Preconditions.checkNotNull(serviceHostIp);
    this.hostIp = serviceHostIp;
    String servicePort = serviceRecord.get(PORT_RECORD_KEY);
    this.port = Integer.parseInt(servicePort);
    String externalId = serviceRecord.get(EXTERNAL_ID_KEY);
    Preconditions.checkNotNull(externalId);
    this.externalId = externalId;
    String computeName = serviceRecord.get(COMPUTE_GROUP_NAME_KEY);
    Preconditions.checkNotNull(computeName);
    this.computeName = computeName;
    String clientToAMTokenString = serviceRecord.get(CLIENT_TO_AM_TOKEN_KEY);
    if (clientToAMTokenString != null) {
      try {
        Token clientToAMToken = new Token();
        clientToAMToken.decodeFromUrlString(clientToAMTokenString);
        this.clientToAMToken = clientToAMToken;
      } catch (IOException err) {
        throw new RuntimeException("Error decoding clientToAMToken", err);
      }
    }
  }

  public ApplicationId getApplicationId() {
    return appId;
  }

  public String getHost() {
    return hostName;
  }

  public String getHostName() {
    return hostName;
  }

  public String getHostIp() {
    return hostIp;
  }

  public int getPort() {
    return port;
  }

  public String getExternalId() { return externalId; }

  public String getComputeName() {
    return computeName;
  }

  /**
   * Token required by TezClients when communicating with the external AM, when security is enabled.
   * @return ClientToAMToken
   */
  public Token getClientToAMToken() { return clientToAMToken; }

  public void setClientToAMToken(Token clientToAMToken) {
    this.clientToAMToken = clientToAMToken;
  }

  private static boolean tokenEquals(Token t1, Token t2) {
    if (t1 == null && t2 == null) {
      return true;
    } else if (t1 != null && t2 != null) {
      return t1.equals(t2);
    } else {
      return false;
    }
  }

  @Override
  public boolean equals(Object other) {
    if(other instanceof AMRecord) {
      AMRecord otherRecord = (AMRecord) other;
      return appId.equals(otherRecord.appId)
          && hostName.equals(otherRecord.hostName)
          && hostIp.equals(otherRecord.hostIp)
          && port == otherRecord.port
          && externalId.equals(otherRecord.externalId)
          && computeName.equals(otherRecord.computeName)
          && tokenEquals(clientToAMToken, ((AMRecord) other).clientToAMToken);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return 31 * appId.hashCode() +
      31 * hostName.hashCode() +
      31 * hostIp.hashCode() +
      31 * externalId.hashCode() +
      31 * computeName.hashCode() +
      31 * port +
      31 * (clientToAMToken == null ? 0 : clientToAMToken.hashCode());
  }

  public ServiceRecord toServiceRecord() {
    ServiceRecord serviceRecord = new ServiceRecord();
    serviceRecord.set(APP_ID_RECORD_KEY, appId);
    serviceRecord.set(HOST_NAME_RECORD_KEY, hostName);
    serviceRecord.set(HOST_IP_RECORD_KEY, hostIp);
    serviceRecord.set(PORT_RECORD_KEY, port);
    serviceRecord.set(EXTERNAL_ID_KEY, externalId);
    serviceRecord.set(COMPUTE_GROUP_NAME_KEY, computeName);
    if (clientToAMToken != null) {
      try {
        serviceRecord.set(CLIENT_TO_AM_TOKEN_KEY, clientToAMToken.encodeToUrlString());
      } catch (IOException err) {
        throw new RuntimeException("Error encoding clientToAMToken", err);
      }
    }
    return serviceRecord;
  }

  public String toString() {
    return toServiceRecord().toString();
  }

}
