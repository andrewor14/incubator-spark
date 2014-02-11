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

import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.util.Utils
import org.apache.spark.ui.UIUtils
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.ExceptionFailure

import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._

import java.util.Date

private[spark] class TaskTable(
    tasksJson: JValue,
    parent: JobProgressUI,
    hasShuffleRead: Boolean,
    hasShuffleWrite: Boolean,
    hasBytesSpilled: Boolean) {

  def toNodeSeq(): Seq[Node] = {
    taskTable()
  }

  private def taskTable[T](): Seq[Node] = {
    val tasks = JValue.asInstanceOf[JArray].arr.toSeq
    UIUtils.listingTable(taskHeaders, taskRow, tasks)
  }

  private def taskHeaders: Seq[String] = {
    Seq("Task Index", "Task ID", "Status", "Locality Level", "Executor", "Launch Time") ++
      Seq("Duration", "GC Time", "Result Ser Time") ++
      {if (hasShuffleRead) Seq("Shuffle Read")  else Nil} ++
      {if (hasShuffleWrite) Seq("Write Time", "Shuffle Write") else Nil} ++
      {if (hasBytesSpilled) Seq("Shuffle Spill (Memory)", "Shuffle Spill (Disk)") else Nil} ++
      Seq("Errors")
  }

  private def taskRow(taskJson: JValue): Seq[Node] = {
    val task = UIUtils.deconstructJsonObjectAsMap(taskJson)
    val status = task("Status")
    val duration = task("Duration").asInstanceOf[]
    val gcTime = task("GC Time").toLong
    val serializationTime = task("Serialization Time").toLong
    val formattedDuration = if (duration > 0) parent.formatDuration(duration) else ""
    val formattedGCTime = if (gcTime > 0) parent.formatDuration(gcTime) else ""
    val formattedSerializationTime = if (serializationTime > 0)
      parent.formatDuration(serializationTime) else ""

    val shuffleReadSize = task("Shuffle Read Size").toLong
    val shuffleReadSortable = if (shuffleReadSize > 0) shuffleReadSize.toString else ""
    val shuffleReadReadable = if (shuffleReadSize > 0) Utils.bytesToString(shuffleReadSize) else ""

    val shuffleWriteSize = task("Shuffle Write Size").toLong
    val shuffleWriteSortable = if (shuffleWriteSize > 0) shuffleWriteSize.toString else ""
    val shuffleWriteReadable = if (shuffleWriteSize > 0)
      Utils.bytesToString(shuffleWriteSize) else ""

    val writeTime = task("Write Time").toLong
    val writeTimeMilli = writeTime / (1000 * 1000)
    val writeTimeSortable = if (writeTime > 0) writeTime.toString else ""
    val writeTimeReadable = if (writeTimeMilli > 0) parent.formatDuration(writeTimeMilli) else ""

    val memoryBytesSpilled = task("Memory Bytes Spilled").toLong
    val memoryBytesSpilledSortable = if (memoryBytesSpilled > 0) memoryBytesSpilled.toString else ""
    val memoryBytesSpilledReadable = if (memoryBytesSpilled > 0)
      Utils.bytesToString(memoryBytesSpilled) else ""

    val diskBytesSpilled = task("Disk Bytes Spilled").toLong
    val diskBytesSpilledSortable = if (diskBytesSpilled > 0) diskBytesSpilled.toString else ""
    val diskBytesSpilledReadable = if (diskBytesSpilled > 0)
      Utils.bytesToString(diskBytesSpilled) else ""

    val exceptions = task("Exceptions")

    def fmtStackTrace(trace: Seq[StackTraceElement]): Seq[Node] =
      trace.map(e => <span style="display:block;">{e.toString}</span>)

    <tr>
      <td>{task("Index")}</td>
      <td>{task("Task ID")}</td>
      <td>{task("Status")}</td>
      <td>{task("Locality")}</td>
      <td>{task("Host")}</td>
      <td>{task("Launch Time")}</td>
      <td sorttable_customkey={task("Duration")}>{formattedDuration}</td>
      <td sorttable_customkey={task("GC Time")}>{formattedGCTime}</td>
      <td sorttable_customkey={task("Serialization Time")}>{formattedSerializationTime}</td>
      {
        if (hasShuffleRead) {
          <td sorttable_customkey={shuffleReadSortable}>
            {shuffleReadReadable}
          </td>
        }
        if (hasShuffleWrite) {
          <td sorttable_customkey={writeTimeSortable}>
            {writeTimeReadable}
          </td>
            <td sorttable_customkey={shuffleWriteSortable}>
              {shuffleWriteReadable}
            </td>
        }
        if (hasBytesSpilled) {
          <td sorttable_customkey={memoryBytesSpilledSortable}>
            {memoryBytesSpilledReadable}
          </td>
          <td sorttable_customkey={diskBytesSpilledSortable}>
            {diskBytesSpilledReadable}
          </td>
        }
      }
      <td>{exception.map(e =>
        <span>
          {e.className} ({e.description})<br/>
          {fmtStackTrace(e.stackTrace)}
        </span>).getOrElse("")}
      </td>
    </tr>
  }
}

private[spark] object TaskTable {

  def constructTaskJson(
      task: (TaskInfo, Option[TaskMetrics], Option[ExceptionFailure]),
      ui: JobProgressUI): JValue = {
    val (info, metrics, exception) = task

    val taskId = info.taskId
    val index = info.index
    val status = info.status
    val locality = info.taskLocality.toString
    val host = info.host
    val launchTime = ui.dateFmt.format(new Date(info.launchTime))
    val duration = if (status == "RUNNING") {
      info.timeRunning(System.currentTimeMillis())
    } else {
      metrics.map(_.executorRunTime.toLong).getOrElse(0L)
    }
    val gcTime = metrics.map(_.jvmGCTime).getOrElse(0L)
    val serializationTime = metrics.map(_.resultSerializationTime).getOrElse(0L)
    val shuffleReadSize = metrics.flatMap(_.shuffleReadMetrics).map(_.remoteBytesRead).getOrElse(0L)
    val shuffleWriteSize = metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleBytesWritten).
      getOrElse(0L)
    val writeTime = metrics.flatMap(_.shuffleWriteMetrics).map(_.shuffleWriteTime).getOrElse(0L)
    val memoryBytesSpilled = metrics.map(_.memoryBytesSpilled).getOrElse(0L)
    val diskBytesSpilled = metrics.map(_.diskBytesSpilled).getOrElse(0L)

    def formatStackTrace(trace: Array[StackTraceElement]) = trace.map(_.toString).toString
    val exceptionsJson = exception.toSeq.map { e =>
      ("Name" -> e.className) ~
      ("Description" -> e.description) ~
      ("Stack Trace" -> formatStackTrace(e.stackTrace))
    }

    ("Task ID" -> taskId) ~
    ("Index" -> index) ~
    ("Status" -> status) ~
    ("Locality" -> locality) ~
    ("Host" -> host) ~
    ("Launch Time" -> launchTime) ~
    ("Duration" -> duration) ~
    ("GC Time" -> gcTime) ~
    ("Serialization Time" -> serializationTime) ~
    ("Shuffle Read Size" -> shuffleReadSize) ~
    ("Shuffle Write Size" -> shuffleWriteSize) ~
    ("Write Time" -> writeTime) ~
    ("Memory Bytes Spilled" -> memoryBytesSpilled) ~
    ("Disk Bytes Spilled" -> diskBytesSpilled) ~
    ("Exceptions" -> exceptionsJson)
  }
}
