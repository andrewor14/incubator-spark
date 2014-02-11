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

import java.util.Date

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ExceptionFailure
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.Page._
import org.apache.spark.util.{Utils, Distribution}
import org.apache.spark.scheduler.TaskInfo
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._

/** Page showing statistics and task list for a given stage */
private[spark] class StagePage(parent: JobProgressUI) {
  def listener = parent.listener
  val dateFmt = parent.dateFmt

  def renderJson(request: HttpServletRequest): JValue = {
    listener.synchronized {
      val stageId = request.getParameter("id").toInt
      if (!listener.stageIdToTaskInfos.contains(stageId)) {
        // No tasks have started yet
        return ("Stage ID" -> stageId) ~ ("Started" -> false)
      }

      val tasks = listener.stageIdToTaskInfos(stageId).toSeq.sortBy(_._1.launchTime)
      val tasksCompleted = tasks.count(_._1.finished)
      val shuffleReadBytes = listener.stageIdToShuffleRead.getOrElse(stageId, 0L)
      val shuffleWriteBytes = listener.stageIdToShuffleWrite.getOrElse(stageId, 0L)
      val memoryBytesSpilled = listener.stageIdToMemoryBytesSpilled.getOrElse(stageId, 0L)
      val diskBytesSpilled = listener.stageIdToDiskBytesSpilled.getOrElse(stageId, 0L)
      var activeTime = 0L
      val now = System.currentTimeMillis()
      listener.stageIdToTasksActive(stageId).foreach(activeTime += _.timeRunning(now))
      val totalTime = listener.stageIdToTime.getOrElse(stageId, 0L) + activeTime

      // Exclude tasks which failed and have incomplete metrics
      val validTasks = tasks.filter(t => t._1.status == "SUCCESS" && (t._2.isDefined))

      // Summary
      var json =
        ("Stage ID" -> stageId) ~
        ("Started" -> true) ~
        ("Tasks Completed" -> tasksCompleted) ~
        ("Shuffle Read Bytes" -> shuffleReadBytes) ~
        ("Shuffle Write Bytes" -> shuffleWriteBytes) ~
        ("Memory Bytes Spilled" -> memoryBytesSpilled) ~
        ("Disk Bytes Spilled" -> diskBytesSpilled) ~
        ("Total Time" -> totalTime) ~
        ("Tasks with Metrics" -> validTasks.size)

      if (validTasks.size > 0) {
        val serializationTimes = validTasks.map { case (info, metrics, exception) =>
          metrics.get.resultSerializationTime.toDouble
        }
        val serviceTimes = validTasks.map { case (info, metrics, exception) =>
          metrics.get.executorRunTime.toDouble
        }
        val gettingResultTimes = validTasks.map { case (info, metrics, exception) =>
          if (info.gettingResultTime > 0) {
            (info.finishTime - info.gettingResultTime).toDouble
          } else {
            0.0
          }
        }
        val schedulerDelays = validTasks.map { case (info, metrics, exception) =>
          val totalExecutionTime = {
            if (info.gettingResultTime > 0) {
              (info.gettingResultTime - info.launchTime).toDouble
            } else {
              (info.finishTime - info.launchTime).toDouble
            }
          }
          totalExecutionTime - metrics.get.executorRunTime
        }
        val shuffleReadSizes = validTasks.map { case(info, metrics, exception) =>
          metrics.get.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
        }
        val shuffleWriteSizes = validTasks.map { case(info, metrics, exception) =>
          metrics.get.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
        }
        val memoryBytesSpilledSizes = validTasks.map { case(info, metrics, exception) =>
          metrics.get.memoryBytesSpilled.toDouble
        }
        val diskBytesSpilledSizes = validTasks.map { case(info, metrics, exception) =>
          metrics.get.diskBytesSpilled.toDouble
        }
        json = json ~
          ("Serialization Times" -> serializationTimes) ~
          ("Service Times" -> serviceTimes) ~
          ("Getting Result Times" -> gettingResultTimes) ~
          ("Scheduler Delays" -> schedulerDelays) ~
          ("Shuffle Read Sizes" -> shuffleReadSizes) ~
          ("Shuffle Write Sizes" -> shuffleWriteSizes) ~
          ("Memory Bytes Spilled Sizes" -> memoryBytesSpilledSizes) ~
          ("Disk Bytes Spilled Sizes" -> diskBytesSpilledSizes)
      }

      val executorsJson = ExecutorTable.constructExecutorsJson(stageId, parent)
      val tasksJson = TaskTable.constructTasksJson(tasks, parent)

      json ~ ("Executors" -> executorsJson) ~ ("Tasks" -> tasksJson)
    }
  }

  def renderHTML(json: JValue): Seq[Node] = {
    val stageId = UIUtils.deconstructJsonInt(json, "Stage ID")
    val started = UIUtils.deconstructJsonBoolean(json, "Started")
    if (!started) {
      val content =
        <div>
          <h4>Summary Metrics</h4> No tasks have started yet
          <h4>Tasks</h4> No tasks have started yet
        </div>
      return UIUtils.headerSparkPage(
        content, parent.sc, "Details for Stage %s".format(stageId), Stages)
    }

    val tasksCompleted = UIUtils.deconstructJsonInt(json, "Tasks Completed")
    val totalTime = UIUtils.deconstructJsonInt(json, "Total Time")
    val shuffleReadBytes = UIUtils.deconstructJsonLong(json, "Shuffle Read Bytes")
    val shuffleWriteBytes = UIUtils.deconstructJsonLong(json, "Shuffle Write Bytes")
    val memoryBytesSpilled = UIUtils.deconstructJsonLong(json, "Memory Bytes Spilled")
    val diskBytesSpilled = UIUtils.deconstructJsonLong(json, "Disk Bytes Spilled")
    val hasShuffleRead = shuffleReadBytes > 0
    val hasShuffleWrite = shuffleWriteBytes > 0
    val hasBytesSpilled = (memoryBytesSpilled > 0 && diskBytesSpilled > 0)

    val summary =
      <div>
        <ul class="unstyled">
          <li>
            <strong>Total task time across all tasks: </strong>{totalTime}
          </li>
          {if (hasShuffleRead)
            <li>
              <strong>Shuffle read: </strong>
              {Utils.bytesToString(shuffleReadBytes)}
            </li>
          }
          {if (hasShuffleWrite)
            <li>
              <strong>Shuffle write: </strong>
              {Utils.bytesToString(shuffleWriteBytes)}
            </li>
          }
          {if (hasBytesSpilled)
          <li>
            <strong>Shuffle spill (memory): </strong>
            {Utils.bytesToString(memoryBytesSpilled)}
          </li>
          <li>
            <strong>Shuffle spill (disk): </strong>
            {Utils.bytesToString(diskBytesSpilled)}
          </li>
          }
        </ul>
      </div>

    // Excludes tasks which failed and have incomplete metrics
    val validTasks = tasks.filter(t => t._1.status == "SUCCESS" && (t._2.isDefined))

    val summaryTable: Option[Seq[Node]] =
      if (validTasks.size == 0) {
        None
      }
      else {
        val serializationTimes = validTasks.map{case (info, metrics, exception) =>
          metrics.get.resultSerializationTime.toDouble}
        val serializationQuantiles = "Result serialization time" +: Distribution(serializationTimes).get.getQuantiles().map(
          ms => parent.formatDuration(ms.toLong))

        val serviceTimes = validTasks.map{case (info, metrics, exception) =>
          metrics.get.executorRunTime.toDouble}
        val serviceQuantiles = "Duration" +: Distribution(serviceTimes).get.getQuantiles().map(
          ms => parent.formatDuration(ms.toLong))

        val gettingResultTimes = validTasks.map{case (info, metrics, exception) =>
          if (info.gettingResultTime > 0) {
            (info.finishTime - info.gettingResultTime).toDouble
          } else {
            0.0
          }
        }
        val gettingResultQuantiles = ("Time spent fetching task results" +:
          Distribution(gettingResultTimes).get.getQuantiles().map(
            millis => parent.formatDuration(millis.toLong)))
        // The scheduler delay includes the network delay to send the task to the worker
        // machine and to send back the result (but not the time to fetch the task result,
        // if it needed to be fetched from the block manager on the worker).
        val schedulerDelays = validTasks.map{case (info, metrics, exception) =>
          val totalExecutionTime = {
            if (info.gettingResultTime > 0) {
              (info.gettingResultTime - info.launchTime).toDouble
            } else {
              (info.finishTime - info.launchTime).toDouble
            }
          }
          totalExecutionTime - metrics.get.executorRunTime
        }
        val schedulerDelayQuantiles = ("Scheduler delay" +:
          Distribution(schedulerDelays).get.getQuantiles().map(
            millis => parent.formatDuration(millis.toLong)))

        def getQuantileCols(data: Seq[Double]) =
          Distribution(data).get.getQuantiles().map(d => Utils.bytesToString(d.toLong))

        val shuffleReadSizes = validTasks.map {
          case(info, metrics, exception) =>
            metrics.get.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
        }
        val shuffleReadQuantiles = "Shuffle Read (Remote)" +: getQuantileCols(shuffleReadSizes)

        val shuffleWriteSizes = validTasks.map {
          case(info, metrics, exception) =>
            metrics.get.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
        }
        val shuffleWriteQuantiles = "Shuffle Write" +: getQuantileCols(shuffleWriteSizes)

        val memoryBytesSpilledSizes = validTasks.map {
          case(info, metrics, exception) =>
            metrics.get.memoryBytesSpilled.toDouble
        }
        val memoryBytesSpilledQuantiles = "Shuffle spill (memory)" +:
          getQuantileCols(memoryBytesSpilledSizes)

        val diskBytesSpilledSizes = validTasks.map {
          case(info, metrics, exception) =>
            metrics.get.diskBytesSpilled.toDouble
        }
        val diskBytesSpilledQuantiles = "Shuffle spill (disk)" +:
          getQuantileCols(diskBytesSpilledSizes)

        val listings: Seq[Seq[String]] = Seq(
          serializationQuantiles,
          serviceQuantiles,
          gettingResultQuantiles,
          schedulerDelayQuantiles,
          if (hasShuffleRead) shuffleReadQuantiles else Nil,
          if (hasShuffleWrite) shuffleWriteQuantiles else Nil,
          if (hasBytesSpilled) memoryBytesSpilledQuantiles else Nil,
          if (hasBytesSpilled) diskBytesSpilledQuantiles else Nil)

        val quantileHeaders = Seq("Metric", "Min", "25th percentile",
          "Median", "75th percentile", "Max")
        def quantileRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>
        Some(listingTable(quantileHeaders, quantileRow, listings, fixedWidth = true))
      }
    val executorTable = new ExecutorTable(parent, stageId)
    val content =
      summary ++
      <h4>Summary Metrics for {numCompleted} Completed Tasks</h4> ++
      <div>{summaryTable.getOrElse("No tasks have reported metrics yet.")}</div> ++
      <h4>Aggregated Metrics by Executor</h4> ++ executorTable.toNodeSeq() ++
      <h4>Tasks</h4> ++ taskTable

    headerSparkPage(content, parent.sc, "Details for Stage %d".format(stageId), Stages)
  }
}
