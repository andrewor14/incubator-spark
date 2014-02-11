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

import javax.servlet.http.HttpServletRequest

import scala.xml.{NodeSeq, Node}

import org.apache.spark.scheduler.{SchedulingMode, StageInfo}
import org.apache.spark.ui.Page._
import org.apache.spark.ui.UIUtils
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._

/** Page showing list of all ongoing and recently finished stages and pools*/
private[spark] class IndexPage(parent: JobProgressUI) {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    val json = renderJson(request)
    renderHTML(json)
  }

  def renderJson(request: HttpServletRequest): JValue = {
    listener.synchronized {
      // Summary
      val activeStages = listener.activeStages.toSeq
      val completedStages = listener.completedStages.toSeq
      val failedStages = listener.failedStages.toSeq
      val totalDuration = parent.formatDuration(System.currentTimeMillis() - listener.sc.startTime)
      val schedulingMode = parent.sc.getSchedulingMode.toString

      // Stages
      val activeStagesJson = StageTable.constructStagesJson(activeStages, parent)
      val completedStagesJson = StageTable.constructStagesJson(completedStages, parent)
      val failedStagesJson = StageTable.constructStagesJson(failedStages, parent)

      // Pools
      val pools = listener.sc.getAllPools
      val poolsJson = PoolTable.constructPoolsJson(pools, parent)

      ("Total Duration" -> totalDuration) ~
      ("Scheduling Mode" -> schedulingMode) ~
      ("Active Stages" -> activeStagesJson) ~
      ("Completed Stages" -> completedStagesJson) ~
      ("Failed Stages" -> failedStagesJson) ~
      ("Pools" -> poolsJson)
    }
  }

  def renderHTML(json: JValue): Seq[Node] = {
    // Summary
    val totalDuration = UIUtils.deconstructJsonString(json, "Total Duration")
    val schedulingMode = UIUtils.deconstructJsonString(json, "Scheduling Mode")

    // Stages
    val activeStages = UIUtils.deconstructJsonArrayAsMap(json, "Active Stages")
    val completedStages = UIUtils.deconstructJsonArrayAsMap(json, "Completed Stages")
    val failedStages = UIUtils.deconstructJsonArrayAsMap(json, "Failed Stages")
    val isFairScheduler = schedulingMode == SchedulingMode.FAIR.toString
    val activeStagesTable = new StageTable(activeStages, parent, isFairScheduler)
    val completedStagesTable = new StageTable(completedStages, parent, isFairScheduler)
    val failedStagesTable = new StageTable(failedStages, parent, isFairScheduler)

    // Pools
    val pools = UIUtils.deconstructJsonArrayAsMap(json, "Pools")
    val poolTable = new PoolTable(pools)

    val summary: NodeSeq =
     <div>
       <ul class="unstyled">
         <li><strong>Total Duration: </strong>{totalDuration}</li>
         <li><strong>Scheduling Mode: </strong>{schedulingMode}</li>
         <li><a href="#active"><strong>Active Stages: </strong></a>{activeStages.size}</li>
         <li><a href="#completed"><strong>Completed Stages:</strong></a>{completedStages.size}</li>
         <li><a href="#failed"><strong>Failed Stages:</strong></a>{failedStages.size}</li>
       </ul>
     </div>

    val content = summary ++
      {if (isFairScheduler) {
         <h4>{pools.size} Fair Scheduler Pools</h4> ++ poolTable.toNodeSeq
      } else {
        Seq()
      }} ++
      <h4 id="active">Active Stages ({activeStages.size})</h4> ++
        activeStagesTable.toNodeSeq ++
      <h4 id="completed">Completed Stages ({completedStages.size})</h4> ++
        completedStagesTable.toNodeSeq ++
      <h4 id ="failed">Failed Stages ({failedStages.size})</h4> ++
        failedStagesTable.toNodeSeq

    UIUtils.headerSparkPage(content, parent.sc, "Spark Stages", Stages)
  }
}
