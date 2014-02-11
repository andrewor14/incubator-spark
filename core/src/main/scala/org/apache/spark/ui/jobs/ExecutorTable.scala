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

package org.apache.spark.ui.jobs

import scala.xml.Node
import scala.collection.mutable

import org.apache.spark.scheduler.SchedulingMode
import org.apache.spark.util.Utils

import net.liftweb.json.JsonAST._
import org.apache.spark.ui.UIUtils

/** Page showing executor summary */
private[spark] class ExecutorTable(val parent: JobProgressUI, val stageId: Int) {
  val listener = parent.listener
  val dateFmt = parent.dateFmt
  val isFairScheduler = listener.sc.getSchedulingMode == SchedulingMode.FAIR

  def toNodeSeq(): Seq[Node] = {
    listener.synchronized {
      executorTable()
    }
  }

  /** Special table which merges two header cells. */
  private def executorTable(): Seq[Node] = {

    val executorTable =

    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>
        <th>Executor ID</th>
        <th>Address</th>
        <th>Task Time</th>
        <th>Total Tasks</th>
        <th>Failed Tasks</th>
        <th>Succeeded Tasks</th>
        <th>Shuffle Read</th>
        <th>Shuffle Write</th>
        <th>Shuffle Spill (Memory)</th>
        <th>Shuffle Spill (Disk)</th>
      </thead>
      <tbody>
        {createExecutorTable()}
      </tbody>
    </table>
  }

  private def executorTable() : Seq[Node] = {
    // make a executor-id -> address map
    val executorIdToAddress = mutable.HashMap[String, String]()
    val storageStatusList = parent.sc.getExecutorStorageStatus
    for (statusId <- 0 until storageStatusList.size) {
      val blockManagerId = parent.sc.getExecutorStorageStatus(statusId).blockManagerId
      val address = blockManagerId.hostPort
      val executorId = blockManagerId.executorId
      executorIdToAddress.put(executorId, address)
    }

    val executorIdToSummary = listener.stageIdToExecutorSummaries.get(stageId)
    executorIdToSummary match {
      case Some(x) => {
        x.toSeq.sortBy(_._1).map{
          case (k,v) => {
            <tr>
              <td>{k}</td>
              <td>{executorIdToAddress.getOrElse(k, "CANNOT FIND ADDRESS")}</td>
              <td>{parent.formatDuration(v.taskTime)}</td>
              <td>{v.failedTasks + v.succeededTasks}</td>
              <td>{v.failedTasks}</td>
              <td>{v.succeededTasks}</td>
              <td>{Utils.bytesToString(v.shuffleRead)}</td>
              <td>{Utils.bytesToString(v.shuffleWrite)}</td>
              <td>{Utils.bytesToString(v.memoryBytesSpilled)}</td>
              <td>{Utils.bytesToString(v.diskBytesSpilled)}</td>
            </tr>
          }
        }
      }
      case _ => { Seq[Node]() }
    }
  }
}

private[spark] object ExecutorTable {

  def constructExecutorsJson(stageId: Int, ui: JobProgressUI): JValue = {
    val executorIdToSummary = ui.listener.stageIdToExecutorSummaries.get(stageId).getOrElse {
      return JNothing
    }

    // Maintain an executor-id -> address map
    val executorIdToAddress = mutable.HashMap[String, String]()
    val storageStatusList = ui.sc.getExecutorStorageStatus
    for (statusId <- 0 until storageStatusList.size) {
      val blockManagerId = ui.sc.getExecutorStorageStatus(statusId).blockManagerId
      val address = blockManagerId.hostPort
      val executorId = blockManagerId.executorId
      executorIdToAddress.put(executorId, address)
    }

    val executorsJson = executorIdToSummary.toSeq.sortBy(_._1).map {
      case (k, v) => {
        val executorId = k
        val address = executorIdToAddress.getOrElse(k, "CANNOT FIND ADDRESS")
        val taskTime = ui.formatDuration(v.taskTime)
        val failedTasks = v.failedTasks
        val succeededTasks = v.succeededTasks
        val shuffleRead = v.shuffleRead
        val shuffleWrite = v.shuffleWrite
        val memoryBytesSpilled = v.memoryBytesSpilled
        val diskBytesSpilled = v.diskBytesSpilled

        val executorFields = Seq("Executor ID", "Address", "Task Time", "Failed Tasks",
          "Succeeded Tasks", "Shuffle Read", "Shuffle Write", "Shuffle Spill (Memory)",
          "Shuffle Spill (Disk)")

        UIUtils.constructJsonObject(
          executorFields.zip(Seq(
            executorId,
            address,
            taskTime,
            failedTasks.toString,
            succeededTasks.toString,
            shuffleRead.toString,
            shuffleWrite.toString,
            memoryBytesSpilled.toString,
            diskBytesSpilled.toString
          ))
        )
      }
    }

    JArray(executorsJson.toList)
  }
}