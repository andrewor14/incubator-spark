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

import org.apache.spark.util.{Utils, Distribution}
import org.apache.spark.ui.UIUtils
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.ExceptionFailure

import net.liftweb.json.JsonAST._
import net.liftweb.json.JsonDSL._

private[spark] class SummaryTable(
  summaryJson: JValue,
  parent: JobProgressUI,
  hasShuffleRead: Boolean,
  hasShuffleWrite: Boolean,
  hasBytesSpilled: Boolean)
{
  def toNodeSeq(): Seq[Node] = {
    def deconstructJsonDoubleArray(identifier: String): Seq[Double] = {
      val jsonList = (summaryJson \ identifier).asInstanceOf[JArray].arr
      jsonList.map(_.asInstanceOf[JDouble].num)
    }
    val serializationQuantiles = "Result serialization time" +:
      deconstructJsonDoubleArray("Serialization Times").map { ms =>
        parent.formatDuration(ms.toLong)
      }
    val serviceQuantiles = "Duration" +: deconstructJsonDoubleArray("Service Times").map { ms =>
      parent.formatDuration(ms.toLong)
    }
    val gettingResultQuantiles = "Time spent fetching task results" +:
      deconstructJsonDoubleArray("Getting Result Times").map { ms =>
        parent.formatDuration(ms.toLong)
      }
    val schedulerDelayQuantiles = "Scheduler delay" +:
      deconstructJsonDoubleArray("Scheduler Delay").map { ms =>
        parent.formatDuration(ms.toLong)
      }
    val shuffleReadQuantiles = "Shuffle Read (Remote)" +:
      deconstructJsonDoubleArray("Shuffle Read Sizes").map { d => Utils.bytesToString(d.toLong) }
    val shuffleWriteQuantiles = "Shuffle Write" +:
      deconstructJsonDoubleArray("Shuffle Write Sizes").map { d => Utils.bytesToString(d.toLong) }
    val memoryBytesSpilledQuantiles = "Shuffle spill (memory)" +:
      deconstructJsonDoubleArray("Memory Bytes Spilled Sizes").map {
        d => Utils.bytesToString(d.toLong)
      }
    val diskBytesSpilledQuantiles = "Shuffle spill (disk)" +:
      deconstructJsonDoubleArray("Disk Bytes Spilled Sizes").map {
        d => Utils.bytesToString(d.toLong)
      }

    val listings: Seq[Seq[String]] = Seq(
      serializationQuantiles,
      serviceQuantiles,
      gettingResultQuantiles,
      schedulerDelayQuantiles,
      if (hasShuffleRead) shuffleReadQuantiles else Nil,
      if (hasShuffleWrite) shuffleWriteQuantiles else Nil,
      if (hasBytesSpilled) memoryBytesSpilledQuantiles else Nil,
      if (hasBytesSpilled) diskBytesSpilledQuantiles else Nil)

    UIUtils.listingTable(quantileHeaders, quantileRow, listings, fixedWidth = true)
  }

  private def quantileHeaders =
    Seq("Metric", "Min", "25th percentile", "Median", "75th percentile", "Max")

  private def quantileRow(data: Seq[String]): Seq[Node] = <tr> {data.map(d => <td>{d}</td>)} </tr>
}

private[spark] object SummaryTable {
  def constructSummaryJson(
    tasks: Seq[(TaskInfo, Option[TaskMetrics], Option[ExceptionFailure])],
    ui: JobProgressUI
  ): JValue = {

    // Raw data
    val serializationTimes = tasks.map { case (info, metrics, exception) =>
      metrics.get.resultSerializationTime.toDouble
    }
    val serviceTimes = tasks.map { case (info, metrics, exception) =>
      metrics.get.executorRunTime.toDouble
    }
    val gettingResultTimes = tasks.map { case (info, metrics, exception) =>
      if (info.gettingResultTime > 0) {
        (info.finishTime - info.gettingResultTime).toDouble
      } else {
        0.0
      }
    }
    val schedulerDelays = tasks.map { case (info, metrics, exception) =>
      val totalExecutionTime = {
        if (info.gettingResultTime > 0) {
          (info.gettingResultTime - info.launchTime).toDouble
        } else {
          (info.finishTime - info.launchTime).toDouble
        }
      }
      totalExecutionTime - metrics.get.executorRunTime
    }
    val shuffleReadSizes = tasks.map { case(info, metrics, exception) =>
      metrics.get.shuffleReadMetrics.map(_.remoteBytesRead).getOrElse(0L).toDouble
    }
    val shuffleWriteSizes = tasks.map { case(info, metrics, exception) =>
      metrics.get.shuffleWriteMetrics.map(_.shuffleBytesWritten).getOrElse(0L).toDouble
    }
    val memoryBytesSpilledSizes = tasks.map { case(info, metrics, exception) =>
      metrics.get.memoryBytesSpilled.toDouble
    }
    val diskBytesSpilledSizes = tasks.map { case(info, metrics, exception) =>
      metrics.get.diskBytesSpilled.toDouble
    }

    // Quantiles
    def makeQuantilesJson(data: Traversable[Double]): JValue = {
      val quantileJson = Distribution(data).get.getQuantiles().map(JDouble(_))
      JArray(quantileJson.toList)
    }
    val serializationQuantiles = makeQuantilesJson(serializationTimes)
    val serviceQuantiles = makeQuantilesJson(serviceTimes)
    val gettingResultQuantiles = makeQuantilesJson(gettingResultTimes)
    val schedulerDelayQuantiles = makeQuantilesJson(schedulerDelays)
    val shuffleReadQuantiles = makeQuantilesJson(shuffleReadSizes)
    val shuffleWriteQuantiles = makeQuantilesJson(shuffleWriteSizes)
    val memoryBytesSpilledQuantiles = makeQuantilesJson(memoryBytesSpilledSizes)
    val diskBytesSpilledQuantiles = makeQuantilesJson(diskBytesSpilledSizes)

    ("Serialization Times" -> serializationQuantiles) ~
    ("Service Times" -> serviceQuantiles) ~
    ("Getting Result Times" -> gettingResultQuantiles) ~
    ("Scheduler Delays" -> schedulerDelayQuantiles) ~
    ("Shuffle Read Sizes" -> shuffleReadQuantiles) ~
    ("Shuffle Write Sizes" -> shuffleWriteQuantiles) ~
    ("Memory Bytes Spilled Sizes" -> memoryBytesSpilledQuantiles) ~
    ("Disk Bytes Spilled Sizes" -> diskBytesSpilledQuantiles)
  }
}