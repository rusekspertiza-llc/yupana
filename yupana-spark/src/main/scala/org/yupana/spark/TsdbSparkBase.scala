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

import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ ConnectionFactory, Result => HBaseResult }
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{ IdentityTableMapper, TableInputFormat, TableMapReduceUtil }
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.yupana.api.query.Query
import org.yupana.api.schema.Schema
import org.yupana.core.dao.{ DictionaryProvider, TSReadingDao, TsdbQueryMetricsDao }
import org.yupana.core.model.{ InternalRow, KeyData }
import org.yupana.core.utils.CloseableIterator
import org.yupana.core.utils.metric.{
  MetricQueryCollector,
  NoMetricCollector,
  PersistentMetricQueryCollector,
  QueryCollectorContext
}
import org.yupana.core.{ QueryContext, TsdbBase }
import org.yupana.hbase.{ DictionaryDaoHBase, HBaseUtils, HdfsFileUtils, TsdbQueryMetricsDaoHBase }
import org.yupana.spark.TsdbSparkBase.createDefaultMetricCollector

object TsdbSparkBase extends StrictLogging {

  @transient var metricsDao: Option[TsdbQueryMetricsDao] = None

  def hbaseConfiguration(config: Config): Configuration = {
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", config.hbaseZookeeper)
    configuration.set("hbase.client.scanner.timeout.period", config.hbaseTimeout.toString)
    if (config.addHdfsToConfiguration) {
      HdfsFileUtils.addHdfsPathToConfiguration(configuration, config.properties)
    }
    configuration
  }

  private def getMetricsDao(config: Config): TsdbQueryMetricsDao = metricsDao match {
    case None =>
      logger.info("TsdbQueryMetricsDao initialization...")
      val hbaseConnection = ConnectionFactory.createConnection(hbaseConfiguration(config))
      val dao = new TsdbQueryMetricsDaoHBase(hbaseConnection, config.hbaseNamespace)
      metricsDao = Some(dao)
      dao
    case Some(d) => d
  }

  private def createDefaultMetricCollector(
      config: Config,
      opName: String = "query"
  ): Query => PersistentMetricQueryCollector = {

    val queryCollectorContext = new QueryCollectorContext(
      metricsDao = () => getMetricsDao(config),
      operationName = opName,
      metricsUpdateInterval = config.metricsUpdateInterval
    )

    { query: Query => new PersistentMetricQueryCollector(queryCollectorContext, query) }
  }
}

abstract class TsdbSparkBase(
    @transient val sparkContext: SparkContext,
    override val prepareQuery: Query => Query,
    conf: Config,
    override val schema: Schema
)(
    metricCollectorCreator: Query => MetricQueryCollector = createDefaultMetricCollector(conf)
) extends TsdbBase
    with Serializable {

  override type Collection[X] = RDD[X]
  override type Result = DataRowRDD

  override val extractBatchSize: Int = conf.extractBatchSize

  HBaseUtils.initStorage(
    ConnectionFactory.createConnection(TsDaoHBaseSpark.hbaseConfiguration(conf)),
    conf.hbaseNamespace,
    schema,
    conf
  )

  override val dictionaryProvider: DictionaryProvider = new SparkDictionaryProvider(conf)

  override val dao: TSReadingDao[RDD, Long] =
    new TsDaoHBaseSpark(sparkContext, schema, conf, dictionaryProvider)

  override def createMetricCollector(query: Query): MetricQueryCollector = {
    if (conf.collectMetrics) {
      metricCollectorCreator(query)
    } else {
      NoMetricCollector
    }
  }

  override def finalizeQuery(
      queryContext: QueryContext,
      data: RDD[Array[Any]],
      metricCollector: MetricQueryCollector
  ): DataRowRDD = {
    metricCollector.setRunningPartitions(data.getNumPartitions)
    val rdd = data.mapPartitions { it =>
      CloseableIterator(it, metricCollector.finishPartition())
    }
    new DataRowRDD(rdd, queryContext)
  }

  def dictionaryRdd(namespace: String, name: String): RDD[(Long, String)] = {
    val scan = DictionaryDaoHBase.getReverseScan

    val job: Job = Job.getInstance(TsDaoHBaseSpark.hbaseConfiguration(conf))
    TableMapReduceUtil.initCredentials(job)
    TableMapReduceUtil.initTableMapperJob(
      DictionaryDaoHBase.getTableName(namespace, name),
      scan,
      classOf[IdentityTableMapper],
      null,
      null,
      job
    )

    val jconf = new JobConf(job.getConfiguration)
    SparkConfUtils.addCredentials(jconf)

    val hbaseRdd = sparkContext.newAPIHadoopRDD(
      job.getConfiguration,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[HBaseResult]
    )

    val rowsRdd = hbaseRdd.flatMap {
      case (_, hbaseResult) =>
        DictionaryDaoHBase.getReversePairFromResult(hbaseResult)
    }
    rowsRdd
  }

  def union(rdds: Seq[DataRowRDD]): DataRowRDD = {
    val rdd = sparkContext.union(rdds.map(_.rows))
    new DataRowRDD(rdd, rdds.head.queryContext)
  }

  override def applyWindowFunctions(
      queryContext: QueryContext,
      keysAndValues: RDD[(KeyData, InternalRow)]
  ): RDD[(KeyData, InternalRow)] = {
    throw new UnsupportedOperationException("Window functions are not supported in TSDB Spark")
  }
}
