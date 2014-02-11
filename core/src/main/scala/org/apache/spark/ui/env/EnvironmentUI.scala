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

package org.apache.spark.ui.env

import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConversions._
import scala.util.Properties
import scala.xml.Node

import org.eclipse.jetty.server.Handler

import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.Page.Environment
import org.apache.spark.SparkContext
import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._

private[spark] class EnvironmentUI(sc: SparkContext) {

  def getHandlers = Seq[(String, Handler)](
    ("/environment", (request: HttpServletRequest) => render(request))
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val json = renderJson(request)
    renderHTML(json)
  }

  def renderJson(request: HttpServletRequest): JValue = {
    // Runtime Information
    val jvmInformation = Seq(
      ("Java Version", "%s (%s)".format(Properties.javaVersion, Properties.javaVendor)),
      ("Java Home", Properties.javaHome),
      ("Scala Version", Properties.versionString),
      ("Scala Home", Properties.scalaHome)
    ).sorted

    // Spark Properties
    val sparkProperties = sc.conf.getAll.sorted

    // System Properties
    val systemProperties = System.getProperties.iterator.toSeq
    val classPathProperties = systemProperties.find { case (k, v) =>
      k == "java.class.path"
    }.getOrElse(("", ""))
    val otherProperties = systemProperties.filter { case (k, v) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Classpath Entries
    val classpathEntries = classPathProperties._2
      .split(sc.conf.get("path.separator", ":"))
      .filterNot(e => e.isEmpty)
      .map(e => (e, "System Classpath"))
    val addedJars = sc.addedJars.iterator.toList.map{case (path, time) => (path, "Added By User")}
    val addedFiles = sc.addedFiles.iterator.toList.map{case (path, time) => (path, "Added By User")}
    val allClasspathEntries = (addedJars ++ addedFiles ++ classpathEntries).sorted

    ("Runtime Information" -> UIUtils.constructJsonFields(jvmInformation)) ~
    ("Spark Properties" -> UIUtils.constructJsonFields(sparkProperties)) ~
    ("System Properties" -> UIUtils.constructJsonFields(otherProperties)) ~
    ("Classpath Entries" -> UIUtils.constructJsonFields(allClasspathEntries))
  }

  def renderHTML(json: JValue): Seq[Node] = {
    def parseJsonFields(name: String): Seq[(String, String)] = {
      val jsonFields = (json \ name).children.asInstanceOf[List[JField]]
      UIUtils.deconstructJsonFields(jsonFields)
    }
    val jvmInformation = parseJsonFields("Runtime Information")
    val sparkProperties = parseJsonFields("Spark Properties")
    val systemProperties = parseJsonFields("System Properties")
    val classpathEntries = parseJsonFields("Classpath Entries")

    val jvmTable =
      UIUtils.listingTable(propertyHeader, row, jvmInformation, fixedWidth = true)
    val sparkPropertyTable =
      UIUtils.listingTable(propertyHeader, row, sparkProperties, fixedWidth = true)
    val otherPropertyTable =
      UIUtils.listingTable(propertyHeader, row, systemProperties, fixedWidth = true)
    val classPathTable =
      UIUtils.listingTable(classPathHeader, row, classpathEntries, fixedWidth = true)

    val content =
      <span>
        <h4>Runtime Information</h4> {jvmTable}
        <h4>Spark Properties</h4> {sparkPropertyTable}
        <h4>System Properties</h4> {otherPropertyTable}
        <h4>Classpath Entries</h4> {classPathTable}
      </span>

    UIUtils.headerSparkPage(content, sc, "Environment", Environment)
  }

  private def propertyHeader = Seq("Name", "Value")
  private def classPathHeader = Seq("Resource", "Source")
  private def row(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
}
