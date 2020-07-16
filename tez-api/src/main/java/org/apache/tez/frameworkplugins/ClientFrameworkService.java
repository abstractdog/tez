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
package org.apache.tez.frameworkplugins;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.registry.AMRegistryClient;

import java.util.Optional;

/*
  FrameworkService that runs code within the client process that is using TezClient
  Bundles together a compatible FrameworkClient and AMRegistryClient
 */
public interface ClientFrameworkService extends FrameworkService {
  //Provide an impl. for org.apache.tez.client.FrameworkClient
  Optional<FrameworkClient> createOrGetFrameworkClient(Configuration conf);
  //Provide an impl. for org.apache.tez.registry.AMRegistryClient
  Optional<AMRegistryClient> createOrGetRegistryClient(Configuration conf);
}
