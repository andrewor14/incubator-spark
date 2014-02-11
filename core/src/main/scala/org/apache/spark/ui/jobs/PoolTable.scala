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
import org.apache.spark.scheduler.Schedulable
import net.liftweb.json.JsonAST._

/** Table showing list of pools */
private[spark] class PoolTable(pools: Seq[Map[String, String]]) {

  def toNodeSeq(): Seq[Node] = {
    poolTable(poolRow, pools)
  }

  private def poolTable(makeRow: (Map[String, String]) => Seq[Node],
                        rows: Seq[Map[String, String]]): Seq[Node] = {
    <table class="table table-bordered table-striped table-condensed sortable table-fixed">
      <thead>
        <th>Pool Name</th>
        <th>Minimum Share</th>
        <th>Pool Weight</th>
        <th>Active Stages</th>
        <th>Running Tasks</th>
        <th>SchedulingMode</th>
      </thead>
      <tbody>
        {rows.map(r => makeRow(r))}
      </tbody>
    </table>
  }

  private def poolRow(values: Map[String, String]) : Seq[Node] = {
    val poolName = values("Pool Name")
    <tr>
      <td>
        <a href={"%s/stages/pool?poolname=%s".format(UIUtils.prependBaseUri(), poolName)}>
          {poolName}
        </a>
      </td>
      <td>{values("Minimum Share")}</td>
      <td>{values("Pool Weight")}</td>
      <td>{values("Active Stages")}</td>
      <td>{values("Running Tasks")}</td>
      <td>{values("Scheduling Mode")}</td>
    </tr>
  }
}

private[spark] object PoolTable {

  def constructPoolJson(pool: Schedulable, ui: JobProgressUI): JValue = {
    val listener = ui.listener
    val name = pool.name
    val minimumShare = pool.minShare
    val weight = pool.weight
    val activeStages = listener.poolToActiveStages.get(name) match {
      case Some(stages) => stages.size
      case None => 0
    }
    val runningTasks = pool.runningTasks
    val schedulingMode = pool.schedulingMode

    val poolFields = Seq("Pool Name", "Minimum Share", "Pool Weight", "Active Stages",
      "Running Tasks", "Scheduling Mode")

    UIUtils.constructJsonObject(
      poolFields.zip(Seq(
        name,
        minimumShare.toString,
        weight.toString,
        activeStages.toString,
        runningTasks.toString,
        schedulingMode.toString
      ))
    )
  }

  def constructPoolsJson(pools: Seq[Schedulable], ui: JobProgressUI): JValue = {
    val poolInfo = pools.map(constructPoolJson(_, ui))
    JArray(poolInfo.toList)
  }
}