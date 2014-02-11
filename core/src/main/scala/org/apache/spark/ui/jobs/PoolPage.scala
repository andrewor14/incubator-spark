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

import scala.xml.Node
import org.apache.spark.ui.Page._

import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import org.apache.spark.ui.UIUtils
import org.apache.spark.scheduler.SchedulingMode

/** Page showing specific pool details */
private[spark] class PoolPage(parent: JobProgressUI) {
  private val listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    val json = renderJson(request)
    renderHTML(json)
  }

  def renderJson(request: HttpServletRequest): JValue = {
    listener.synchronized {
      val poolName = request.getParameter("poolname")
      val schedulingMode = parent.sc.getSchedulingMode.toString
      val activeStages = listener.poolToActiveStages.get(poolName).toSeq.flatten
      val activeStagesJson = StageTable.constructStagesJson(activeStages, parent)
      val pools = Seq(listener.sc.getPoolForName(poolName).get)
      val poolsJson = PoolTable.constructPoolsJson(pools, parent)

      ("Pool Name" -> poolName) ~
      ("Scheduling Mode" -> schedulingMode) ~
      ("Active Stages" -> activeStagesJson) ~
      ("Pools" -> poolsJson)
    }
  }

  def renderHTML(json: JValue): Seq[Node] = {
    val poolName = UIUtils.deconstructJsonString(json, "Pool Name")
    val schedulingMode = UIUtils.deconstructJsonString(json, "Scheduling Mode")
    val isFairScheduler = schedulingMode == SchedulingMode.FAIR.toString
    val activeStages = UIUtils.deconstructJsonArrayAsMap(json, "Active Stages")
    val activeStagesTable = new StageTable(activeStages, parent, isFairScheduler)
    val pools = UIUtils.deconstructJsonArrayAsMap(json, "Pools")
    val poolTable = new PoolTable(pools)

    val content = <h4>Summary </h4> ++ poolTable.toNodeSeq() ++
                  <h4>{activeStages.size} Active Stages</h4> ++ activeStagesTable.toNodeSeq()
    UIUtils.headerSparkPage(content, parent.sc, "Fair Scheduler Pool: " + poolName, Stages)
  }
}
