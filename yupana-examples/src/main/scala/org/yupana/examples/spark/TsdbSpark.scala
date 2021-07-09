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

package org.yupana.examples.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.yupana.api.query.{ DataPoint, Query }
import org.yupana.api.schema.{ ExternalLink, Schema, Table }
import org.yupana.core.ExternalLinkService
import org.yupana.core.cache.CacheFactory
import org.yupana.core.dao.{ ChangelogDao, TSDao }
import org.yupana.examples.externallinks.ExternalLinkRegistrator
import org.yupana.spark.{ Config, TsDaoHBaseSpark, TsdbSparkBase }

object TsdbSpark {
  var externalLinks = Map.empty[String, ExternalLinkService[_ <: ExternalLink]]
}

class TsdbSpark(
    override val dao: TSDao[RDD, Long],
    val changelogDao: ChangelogDao,
    sparkContext: SparkContext,
    prepareQuery: Query => Query,
    conf: Config,
    schema: Schema
) extends TsdbSparkBase(sparkContext, prepareQuery, conf, schema)() {

  @transient lazy val elRegistrator =
    new ExternalLinkRegistrator(
      this,
      TsDaoHBaseSpark.hbaseConfiguration(conf),
      conf.hbaseNamespace,
      conf.properties
    )

  override def linkService(el: ExternalLink): ExternalLinkService[_ <: ExternalLink] = {
    if (!TsdbSpark.externalLinks.contains(el.linkName)) {
      logger.info(s"${el.linkName} is not registered yet, so let's create and register it")
      CacheFactory.init(conf.properties, conf.hbaseNamespace)
      elRegistrator.registerExternalLink(el)
    }
    TsdbSpark.externalLinks
      .getOrElse(el.linkName, throw new Exception(s"Can't find catalog ${el.linkName}: ${el.fields}"))
  }

  def registerExternalLink(catalog: ExternalLink, catalogService: ExternalLinkService[_ <: ExternalLink]): Unit = {
    TsdbSpark.externalLinks += (catalog.linkName -> catalogService)
  }

  /**
    * Save DataPoints into table.
    *
    * @note This method takes table as a parameter, and saves only data points related to this table. All data points
    *       related to another tables are ignored.
    *
    * @param dataPointsRDD data points to be saved
    * @param table table to store data points
    */
  def writeRDD(dataPointsRDD: RDD[DataPoint], table: Table, username: String): Unit = {
    val filtered = dataPointsRDD.filter(_.table == table)
    val updatedIntervals = dao.put(filtered, username).collect()
    changelogDao.putUpdatesIntervals(updatedIntervals)
  }
}
