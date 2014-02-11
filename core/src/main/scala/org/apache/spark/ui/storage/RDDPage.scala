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

import org.apache.spark.storage.{RDDInfo, BlockId, StorageStatus, StorageUtils}
import org.apache.spark.storage.BlockManagerMasterActor.BlockStatus
import org.apache.spark.ui.UIUtils._
import org.apache.spark.ui.Page._
import org.apache.spark.util.Utils
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import org.apache.spark.ui.UIUtils

/** Page showing storage details for a given RDD */
private[spark] class RDDPage(parent: BlockManagerUI) {
  val sc = parent.sc

  def render(request: HttpServletRequest): Seq[Node] = {
    val json = renderJson(request)
    renderHTML(json)
  }

  def renderJson(request: HttpServletRequest): JValue = {
    val id = request.getParameter("id").toInt
    val storageStatusList = sc.getExecutorStorageStatus
    val filteredStorageStatusList = StorageUtils.filterStorageStatusByRDD(storageStatusList, id)
    val rddInfo = StorageUtils.rddInfoFromStorageStatus(filteredStorageStatusList, sc).head

    // RDD info
    val rddJson = UIUtils.constructJsonFields(getRDDInfo(rddInfo))

    // Worker info
    val workersJson = JArray(filteredStorageStatusList.map { status =>
      UIUtils.constructJsonObject(getWorkerInfo(id, status))
    }.toList)

    // Block info
    val blockStatuses = filteredStorageStatusList.flatMap(_.blocks).toArray.
      sortWith(_._1.name < _._1.name)
    val blockLocations = StorageUtils.blockLocationsFromStorageStatus(filteredStorageStatusList)
    val blocksJson = JArray(blockStatuses.map { case(id, status) =>
      val blockInfo = getBlockInfo(id, status, blockLocations.get(id).getOrElse(Seq("UNKNOWN")))
      UIUtils.constructJsonObject(blockInfo)
    }.toList)

    ("RDD Information" -> rddJson) ~
    ("Worker Information" -> workersJson) ~
    ("Block Information" -> blocksJson)
  }

  def renderHTML(json: JValue): Seq[Node] = {
    // Parse worker info
    val workersJson = (json \ "Worker Information").asInstanceOf[JArray].arr
    val workers = workersJson.map(UIUtils.deconstructJsonObjectAsMap)
    val workerTable = listingTable(workerHeader, workerRow, workers)

    // Parse block info
    val blocksJson = (json \ "Block Information").asInstanceOf[JArray].arr
    val blocks = blocksJson.map(UIUtils.deconstructJsonObjectAsMap)
    val blockTable = listingTable(blockHeader, blockRow, blocks)

    // Parse RDD info
    val rddJson = (json \ "RDD Information").children.asInstanceOf[List[JField]]
    val rddInfo = UIUtils.deconstructJsonFieldsAsMap(rddJson)

    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li>
              <strong>Storage Level:</strong> {rddInfo("Storage Level")}
            </li>
            <li>
              <strong>Cached Partitions:</strong> {rddInfo("Cached Partitions")}
            </li>
            <li>
              <strong>Total Partitions:</strong> {rddInfo("Total Partitions")}
            </li>
            <li>
              <strong>Memory Size:</strong> {Utils.bytesToString(rddInfo("Memory Size").toLong)}
            </li>
            <li>
              <strong>Disk Size:</strong> {Utils.bytesToString(rddInfo("Disk Size").toLong)}
            </li>
          </ul>
        </div>
      </div>

      <div class="row-fluid">
        <div class="span12">
          <h4> Data Distribution on {workers.size} Executors </h4>
          {workerTable}
        </div>
      </div>

      <div class="row-fluid">
        <div class="span12">
          <h4> {blocks.size} Partitions </h4>
          {blockTable}
        </div>
      </div>;

    headerSparkPage(content, parent.sc, "RDD Storage Info for " + rddInfo("Name"), Storage)
  }

  private def workerHeader = Seq("Host", "Memory Usage", "Disk Usage")
  private def blockHeader =
    Seq("Block Name", "Storage Level", "Size in Memory", "Size on Disk", "Executors")

  private def workerRow(values: Map[String, String]): Seq[Node] = {
    <tr>
      <td>{values("Host")}</td>
      <td>
        {Utils.bytesToString(values("Memory Usage").toLong)}
        ({Utils.bytesToString(values("Memory Remaining").toLong)} Remaining)
      </td>
      <td>{Utils.bytesToString(values("Disk Usage").toLong)}</td>
    </tr>
  }

  private def blockRow(values: Map[String, String]): Seq[Node] = {
    <tr>
      <td>{values("Block Name")}</td>
      <td>{values("Storage Level")}</td>
      <td sorttable_customkey={values("Size in Memory")}>
        {Utils.bytesToString(values("Size in Memory").toLong)}
      </td>
      <td sorttable_customkey={values("Size on Disk")}>
        {Utils.bytesToString(values("Size on Disk").toLong)}
      </td>
      <td>
        {values("Executors").split(",").map(l => <span>{l}<br/></span>)}
      </td>
    </tr>
  }

  private def getRDDInfo(rddInfo: RDDInfo): Seq[(String, String)] = {
    val name = rddInfo.name
    val storageLevel = rddInfo.storageLevel.description
    val cachedPartitions = rddInfo.numCachedPartitions
    val totalPartitions = rddInfo.numPartitions
    val memorySize = rddInfo.memSize
    val diskSize = rddInfo.diskSize

    val rddFields = Seq("Name", "Storage Level", "Cached Partitions", "Total Partitions",
      "Memory Size", "Disk Size")

    rddFields.zip(Seq(
      name,
      storageLevel,
      cachedPartitions.toString,
      totalPartitions.toString,
      memorySize.toString,
      diskSize.toString
    ))
  }

  private def getWorkerInfo(rddId: Int, status: StorageStatus): Seq[(String, String)] = {
    val host = status.blockManagerId.host + ":" + status.blockManagerId.port
    val memUsed = status.memUsedByRDD(rddId)
    val memRemaining = status.memRemaining
    val diskUsed = status.diskUsedByRDD(rddId)

    // Not the same as header row of the worker info table; this one has "Memory Remaining"
    val workerFields = Seq("Host", "Memory Usage", "Memory Remaining", "Disk Usage")

    workerFields.zip(Seq(
      host,
      memUsed.toString,
      memRemaining.toString,
      diskUsed.toString
    ))
  }

  private def getBlockInfo(blockId: BlockId, status: BlockStatus, locations: Seq[String])
  : Seq[(String, String)] = {
    val storageLevel = status.storageLevel.description
    val memorySize = status.memSize
    val diskSize = status.diskSize
    val executors = locations.mkString(",")
    val blockFields = blockHeader

    blockFields.zip(Seq(
      blockId.toString,
      storageLevel,
      memorySize.toString,
      diskSize.toString,
      executors.toString
    ))
  }
}
