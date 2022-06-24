/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.worker;

import java.io.Closeable;
import java.util.List;

import org.apache.hadoop.fs.Path;

/** Implementations are responsible for creating and obtaining path information about partitions.
 * @deprecated as of Hive 3.0.0
 */
@Deprecated
interface PartitionHelper extends Closeable {

  /** Return the location of the partition described by the provided values. */
  Path getPathForPartition(List<String> newPartitionValues) throws WorkerException;

  /** Create the partition described by the provided values if it does not exist already. */
  void createPartitionIfNotExists(List<String> newPartitionValues) throws WorkerException;

}