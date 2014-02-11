/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ui.storage

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.storage.{RDDInfo, StorageUtils}
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.Page._
import org.apache.spark.util.Utils
import net.liftweb.json.JsonAST._

/** Page showing list of RDD's currently stored in the cluster */
private[spark] class IndexPage(parent: BlockManagerUI) {
  val sc = parent.sc

  def render(request: HttpServletRequest): Seq[Node] = {
    val json = renderJson(request)
    renderHTML(json)
  }

  def renderJson(request: HttpServletRequest): JValue = {
    val storageStatusList = sc.getExecutorStorageStatus
    val rddStatus = StorageUtils.rddInfoFromStorageStatus(storageStatusList, sc)
    val rddInfo = rddStatus.map(getRDDInfo)
    JArray(rddInfo.map(UIUtils.constructJsonObject).toList)
  }

  def renderHTML(json: JValue): Seq[Node] = {
    val rddInfoJson = json.asInstanceOf[JArray].arr
    val rddInfo = rddInfoJson.map(UIUtils.deconstructJsonObjectAsMap)
    val content = UIUtils.listingTable(rddHeader, rddRow, rddInfo)
    UIUtils.headerSparkPage(content, parent.sc, "Storage", Storage)
  }

  private def rddHeader = Seq("RDD Name", "Storage Level", "Cached Partitions", "Fraction Cached",
    "Size in Memory", "Size on Disk")

  private def rddRow(values: Map[String, String]): Seq[Node] = {
    <tr>
      <td>
        <a href={values("RDD Link")}>
          {values("RDD Name")}
        </a>
      </td>
      <td>{values("Storage Level")}
      </td>
      <td>{values("Cached Partitions")}</td>
      <td>{values("Fraction Cached")}</td>
      <td>{Utils.bytesToString(values("Size in Memory").toLong)}</td>
      <td>{Utils.bytesToString(values("Size on Disk").toLong)}</td>
    </tr>
  }

  private def getRDDInfo(rdd: RDDInfo): Seq[(String, String)] = {
    val name = rdd.name
    val link = "%s/storage/rdd?id=%s".format(UIUtils.prependBaseUri(),rdd.id)
    val storageLevel = rdd.storageLevel.description
    val cachedPartitions = rdd.numCachedPartitions
    val fractionCached = {"%.0f%%".format(rdd.numCachedPartitions * 100.0 / rdd.numPartitions)}
    val memorySize = rdd.memSize
    val diskSize = rdd.diskSize

    // Not the same as header row in RDD Info table; this one has "RDD Link"
    val rddFields = Seq("RDD Name", "RDD Link", "Storage Level", "Cached Partitions",
      "Fraction Cached", "Size in Memory", "Size on Disk")

    rddFields.zip(Seq(
      name,
      link,
      storageLevel,
      cachedPartitions.toString,
      fractionCached,
      memorySize.toString,
      diskSize.toString
    ))
  }
}
