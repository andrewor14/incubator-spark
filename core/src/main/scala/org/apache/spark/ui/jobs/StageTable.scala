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

import org.apache.spark.ui.UIUtils
import org.apache.spark.util.Utils
import org.apache.spark.scheduler.{TaskInfo, StageInfo}
import java.util.Date
import scala.collection.mutable
import net.liftweb.json.JsonAST._

/** Page showing list of all ongoing and recently finished stages */
private[spark] class StageTable(stages: Seq[Map[String, String]],
                                parent: JobProgressUI,
                                isFairScheduler: Boolean) {

  def toNodeSeq(): Seq[Node] = {
    stageTable(stageRow, stages)
  }

  /** Special table which merges two header cells. */
  private def stageTable[T](makeRow: (T, Boolean) => Seq[Node], rows: Seq[T]): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>
        <th>Stage Id</th>
        {if (isFairScheduler) {<th>Pool Name</th>} else {}}
        <th>Description</th>
        <th>Submitted</th>
        <th>Duration</th>
        <th>Tasks: Succeeded/Total</th>
        <th>Shuffle Read</th>
        <th>Shuffle Write</th>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r, isFairScheduler))}
      </tbody>
    </table>
  }

  private def makeProgressBar(started: Int, completed: Int, failed: String, total: Int): Seq[Node] = {
    val completeWidth = "width: %s%%".format((completed.toDouble/total)*100)
    val startWidth = "width: %s%%".format((started.toDouble/total)*100)

    <div class="progress">
      <span style="text-align:center; position:absolute; width:100%;">
        {completed}/{total} {failed}
      </span>
      <div class="bar bar-completed" style={completeWidth}></div>
      <div class="bar bar-running" style={startWidth}></div>
    </div>
  }

  private def stageRow(values: Map[String, String], isFairScheduler: Boolean): Seq[Node] = {
    val stageId = values("Stage ID")
    val poolName = values("Pool Name")
    val nameLink = {
      <a href={"%s/stages/stage?id=%s".format(UIUtils.prependBaseUri(),stageId)}>
        {values("Stage Name")}
      </a>
    }
    val description = values("description") match {
      case "" => nameLink
      case d => <div><em>{d}</em></div><div>{nameLink}</div>
    }
    val submissionTime = values("Submission Time")
    val durationSortable = values("Duration")
    val duration = durationSortable.toLong match {
      case -1 => "Unknown"
      case d => parent.formatDuration(d)
    }
    val progressBar = makeProgressBar(values("Started Tasks").toInt,
      values("Completed Tasks").toInt, values("Failed Tasks"), values("Total Tasks").toInt)
    val shuffleReadSortable = values("Shuffle Read")
    val shuffleRead = shuffleReadSortable.toLong match {
      case 0 => ""
      case b => Utils.bytesToString(b)
    }
    val shuffleWriteSortable = values("Shuffle Write")
    val shuffleWrite = shuffleWriteSortable.toLong match {
      case 0 => ""
      case b => Utils.bytesToString(b)
    }

    <tr>
      <td>{stageId}</td>
      {if (isFairScheduler) {
          <td>
            <a href={"%s/stages/pool?poolname=%s".format(UIUtils.prependBaseUri(), poolName)}>
              {poolName}
            </a>
          </td>
      }}
      <td>{description}</td>
      <td valign="middle">{submissionTime}</td>
      <td sorttable_customkey={durationSortable}>{duration}</td>
      <td class="progress-cell">{progressBar}</td>
      <td sorttable_customekey={shuffleReadSortable}>{shuffleRead}</td>
      <td sorttable_customekey={shuffleWriteSortable}>{shuffleWrite}</td>
    </tr>
  }
}

private[spark] object StageTable {

  /** Extract UI information from stage info */
  def constructStageJson(s: StageInfo, ui: JobProgressUI): JValue = {
    val listener = ui.listener
    val stageId = s.stageId
    val stageName = s.name
    val poolName = listener.stageIdToPool(stageId)
    val description = listener.stageIdToDescription(stageId)
    val submissionTime = s.submissionTime match {
      case Some(t) => ui.dateFmt.format(new Date(t))
      case None => "Unknown"
    }
    val finishTime = s.completionTime.getOrElse(System.currentTimeMillis())
    val duration =  s.submissionTime.map(t => finishTime - t).getOrElse(-1)
    val shuffleRead = listener.stageIdToShuffleRead.getOrElse(stageId, 0L)
    val shuffleWrite = listener.stageIdToShuffleWrite.getOrElse(stageId, 0L)
    val startedTasks = listener.stageIdToTasksActive.getOrElse(stageId,
      mutable.HashSet[TaskInfo]()).size
    val completedTasks = listener.stageIdToTasksComplete.getOrElse(stageId, 0)
    val failedTasks = listener.stageIdToTasksFailed.getOrElse(stageId, 0) match {
      case f if f > 0 => "(%s failed)".format(f)
      case _ => ""
    }
    val totalTasks = s.numTasks

    val stageFields = Seq("Stage ID", "Stage Name", "Pool Name", "Description", "Submission Time",
      "Duration", "Shuffle Read", "Shuffle Write", "Started Tasks", "Completed Tasks",
      "Failed Tasks", "Total Tasks")

    UIUtils.constructJsonObject(
      stageFields.zip(Seq(
        stageId.toString,
        stageName,
        poolName,
        description,
        submissionTime,
        duration.toString,
        shuffleRead.toString,
        shuffleWrite.toString,
        startedTasks.toString,
        completedTasks.toString,
        failedTasks.toString,
        totalTasks.toString
      ))
    )
  }

  def constructStagesJson(stages: Seq[StageInfo], ui: JobProgressUI): JValue = {
    val stagesJson = stages.sortBy(_.submissionTime).reverse.map { s =>
      constructStageJson(s, ui)
    }
    JArray(stagesJson.toList)
  }
}