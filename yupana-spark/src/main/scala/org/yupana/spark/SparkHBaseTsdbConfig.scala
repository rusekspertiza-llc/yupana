/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.spark

import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.spark.SparkConf
import org.yupana.hbase.TSDBHBaseConfig

class SparkHBaseTsdbConfig(@transient val sparkConf: SparkConf)
    extends TSDBHBaseConfig(
      hbaseNamespace = sparkConf.getOption("tsdb.hbase.namespace").getOrElse("default"),
      collectMetrics = sparkConf.getBoolean("analytics.tsdb.collect-metrics", defaultValue = true),
      metricsUpdateInterval = sparkConf.getInt("analytics.tsdb.metrics-update-interval", 30000),
      extractBatchSize = sparkConf.getInt("analytics.tsdb.extract-batch-size", 10000),
      putBatchSize = sparkConf.getInt("analytics.tsdb.put-batch-size", 1000),
      putEnabled = true,
      maxRegions = sparkConf.getInt("spark.hbase.regions.initial.max", 50),
      reduceLimit = Int.MaxValue,
      needCheckSchema = true,
      compression = sparkConf.getOption("tsdb.hbase.compression").getOrElse(Algorithm.SNAPPY.getName),
      hbaseZookeeper = sparkConf.get("hbase.zookeeper"),
      hbaseWriteBufferSize = sparkConf.getOption("hbase.write.buffer").map(_.trim).filter(_.nonEmpty).map(_.toLong),
      settings = SparkConfSettings(sparkConf)
    )
    with Serializable {

  val hbaseTimeout: Int = sparkConf.getInt("analytics.tsdb.rollup-job.hbase.timeout", 900000) // 15 minutes
  val addHdfsToConfiguration: Boolean =
    sparkConf.getBoolean("analytics.jobs.add-hdfs-to-configuration", defaultValue = false)
  val minHBaseScanPartitions: Int = sparkConf.getInt("analytics.tsdb.spark.min-hbase-scan-partitions", 50)
}