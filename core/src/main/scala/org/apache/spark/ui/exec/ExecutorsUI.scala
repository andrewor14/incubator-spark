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

package org.apache.spark.ui.exec

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.{HashMap, HashSet}
import scala.xml.Node

import org.eclipse.jetty.server.Handler

import org.apache.spark.{Logging, ExceptionFailure, SparkContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{SparkListenerTaskStart, SparkListenerTaskEnd, SparkListener}
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.Page.Executors
import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._

private[spark] class ExecutorsUI(val sc: SparkContext) extends Logging {

  private var _listener: Option[ExecutorsListener] = None
  def listener = _listener.get

  def start() {
    _listener = Some(new ExecutorsListener)
    sc.addSparkListener(listener)
  }

  def getHandlers = Seq[(String, Handler)](
    ("/executors", (request: HttpServletRequest) => render(request))
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val json = renderJson(request)
    renderHTML(json)
  }

  def renderJson(request: HttpServletRequest): JValue = {
    val storageStatusList = sc.getExecutorStorageStatus
    val maxMem = storageStatusList.map(_.maxMem).fold(0L)(_+_)
    val memUsed = storageStatusList.map(_.memUsed()).fold(0L)(_+_)
    val diskSpaceUsed = storageStatusList.flatMap(_.blocks.values.map(_.diskSize)).fold(0L)(_+_)
    val execInfo = for (statusId <- 0 until storageStatusList.size) yield getExecInfo(statusId)
    val execInfoJson = JArray(execInfo.map(UIUtils.constructJsonObject).toList)

    ("Memory Available" -> maxMem) ~
    ("Memory Used" -> memUsed) ~
    ("Disk Space Used" -> diskSpaceUsed) ~
    ("Executor Information" -> execInfoJson)
  }

  def renderHTML(json: JValue): Seq[Node] = {
    val maxMem = (json \ "Memory Available").asInstanceOf[JInt].num
    val memUsed = (json \ "Memory Used").asInstanceOf[JInt].num
    val diskSpaceUsed = (json \ "Disk Space Used").asInstanceOf[JInt].num
    val execInfoJson = (json \ "Executor Information").asInstanceOf[JArray].arr
    val execInfo = execInfoJson.map(UIUtils.deconstructJsonObjectAsMap)
    val execTable = UIUtils.listingTable(execHeader, execRow, execInfo)

    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li><strong>Memory:</strong>
              {Utils.bytesToString(memUsed.toLong)} Used
              ({Utils.bytesToString(maxMem.toLong)} Total) </li>
            <li><strong>Disk:</strong> {Utils.bytesToString(diskSpaceUsed.toLong)} Used </li>
          </ul>
        </div>
      </div>
      <div class = "row">
        <div class="span12">
          {execTable}
        </div>
      </div>;

    UIUtils.headerSparkPage(content, sc, "Executors (" + execInfo.size + ")", Executors)
  }

  private def execHeader = Seq("Executor ID", "Address", "RDD Blocks", "Memory Used", "Disk Used",
    "Active Tasks", "Failed Tasks", "Complete Tasks", "Total Tasks", "Task Time", "Shuffle Read",
    "Shuffle Write")

  private def execRow(values: Map[String, String]) = {
    <tr>
      <td>{values("Executor ID")}</td>
      <td>{values("Address")}</td>
      <td>{values("RDD Blocks")}</td>
      <td sorttable_customkey={values("Memory Used")}>
        {Utils.bytesToString(values("Memory Used").toLong)} /
        {Utils.bytesToString(values("Memory Available").toLong)}
      </td>
      <td sorttable_customkey={values("Disk Used")}>
        {Utils.bytesToString(values("Disk Used").toLong)}
      </td>
      <td>{values("Active Tasks")}</td>
      <td>{values("Failed Tasks")}</td>
      <td>{values("Complete Tasks")}</td>
      <td>{values("Total Tasks")}</td>
      <td>{Utils.msDurationToString(values("Task Time").toLong)}</td>
      <td>{Utils.bytesToString(values("Shuffle Read").toLong)}</td>
      <td>{Utils.bytesToString(values("Shuffle Write").toLong)}</td>
    </tr>
  }

  private def getExecInfo(statusId: Int): Seq[(String, String)] = {
    val status = sc.getExecutorStorageStatus(statusId)
    val execId = status.blockManagerId.executorId
    val hostPort = status.blockManagerId.hostPort
    val rddBlocks = status.blocks.size.toString
    val memUsed = status.memUsed().toString
    val maxMem = status.maxMem.toString
    val diskUsed = status.diskUsed().toString
    val activeTasks = listener.executorToTasksActive.getOrElse(execId, HashSet.empty[Long]).size
    val failedTasks = listener.executorToTasksFailed.getOrElse(execId, 0)
    val completedTasks = listener.executorToTasksComplete.getOrElse(execId, 0)
    val totalTasks = activeTasks + failedTasks + completedTasks
    val totalDuration = listener.executorToDuration.getOrElse(execId, 0)
    val totalShuffleRead = listener.executorToShuffleRead.getOrElse(execId, 0)
    val totalShuffleWrite = listener.executorToShuffleWrite.getOrElse(execId, 0)

    // Not the same as execHead; this one has "Memory available"
    val execFields = Seq("Executor ID", "Address", "RDD Blocks", "Memory Used", "Memory Available",
      "Disk Used", "Active Tasks", "Failed Tasks", "Complete Tasks", "Total Tasks", "Task Time",
      "Shuffle Read", "Shuffle Write")

    execFields.zip(Seq(
      execId,
      hostPort,
      rddBlocks,
      memUsed,
      maxMem,
      diskUsed,
      activeTasks.toString,
      failedTasks.toString,
      completedTasks.toString,
      totalTasks.toString,
      totalDuration.toString,
      totalShuffleRead.toString,
      totalShuffleWrite.toString
    ))
  }

  private[spark] class ExecutorsListener extends SparkListener with Logging {
    val executorToTasksActive = HashMap[String, HashSet[TaskInfo]]()
    val executorToTasksComplete = HashMap[String, Int]()
    val executorToTasksFailed = HashMap[String, Int]()
    val executorToDuration = HashMap[String, Long]()
    val executorToShuffleRead = HashMap[String, Long]()
    val executorToShuffleWrite = HashMap[String, Long]()

    override def onTaskStart(taskStart: SparkListenerTaskStart) {
      val eid = taskStart.taskInfo.executorId
      val activeTasks = executorToTasksActive.getOrElseUpdate(eid, new HashSet[TaskInfo]())
      activeTasks += taskStart.taskInfo
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
      val eid = taskEnd.taskInfo.executorId
      val activeTasks = executorToTasksActive.getOrElseUpdate(eid, new HashSet[TaskInfo]())
      val newDuration = executorToDuration.getOrElse(eid, 0L) + taskEnd.taskInfo.duration
      executorToDuration.put(eid, newDuration)

      activeTasks -= taskEnd.taskInfo
      val (failureInfo, metrics): (Option[ExceptionFailure], Option[TaskMetrics]) =
        taskEnd.reason match {
          case e: ExceptionFailure =>
            executorToTasksFailed(eid) = executorToTasksFailed.getOrElse(eid, 0) + 1
            (Some(e), e.metrics)
          case _ =>
            executorToTasksComplete(eid) = executorToTasksComplete.getOrElse(eid, 0) + 1
            (None, Option(taskEnd.taskMetrics))
        }

      // update shuffle read/write
      if (null != taskEnd.taskMetrics) {
        taskEnd.taskMetrics.shuffleReadMetrics.foreach(shuffleRead =>
          executorToShuffleRead.put(eid, executorToShuffleRead.getOrElse(eid, 0L) +
            shuffleRead.remoteBytesRead))

        taskEnd.taskMetrics.shuffleWriteMetrics.foreach(shuffleWrite =>
          executorToShuffleWrite.put(eid, executorToShuffleWrite.getOrElse(eid, 0L) +
            shuffleWrite.shuffleBytesWritten))
      }
    }
  }
}
