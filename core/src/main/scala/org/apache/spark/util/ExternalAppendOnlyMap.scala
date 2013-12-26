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

package org.apache.spark.util

import java.io._
import java.text.DecimalFormat

import scala.Some
import scala.Predef._
import scala.collection.mutable.{ArrayBuffer, PriorityQueue}
import scala.util.Random

/**
 * A wrapper for SpillableAppendOnlyMap that handles two cases:
 *
 * (1)  If a mergeCombiners function is specified, merge values into combiners before
 *      disk spill, as it is possible to merge the resulting combiners later.
 *
 * (2)  Otherwise, group values of the same key together before disk spill, and merge
 *      them into combiners only after reading them back from disk.
 */
class ExternalAppendOnlyMap[K, V, C](
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    memoryThresholdMB: Long = 1024)
  extends Iterable[(K, C)] with Serializable {

  private val mergeBeforeSpill: Boolean = mergeCombiners != null

  private val map: SpillableAppendOnlyMap[K, V, _, C] = {
    if (mergeBeforeSpill) {
      new SpillableAppendOnlyMap[K, V, C, C] (createCombiner,
        mergeValue, mergeCombiners, Predef.identity, memoryThresholdMB)
    } else {
      val createGroup: (V => ArrayBuffer[V]) = value => ArrayBuffer[V](value)
      new SpillableAppendOnlyMap[K, V, ArrayBuffer[V], C] (createGroup,
        mergeValueIntoGroup, mergeGroups, combineGroup, memoryThresholdMB)
    }
  }

  def insert(key: K, value: V): Unit = map.insert(key, value)

  override def iterator: Iterator[(K, C)] = map.iterator

  private def mergeValueIntoGroup(group: ArrayBuffer[V], value: V): ArrayBuffer[V] = {
    group += value
    group
  }
  private def mergeGroups(group1: ArrayBuffer[V], group2: ArrayBuffer[V]): ArrayBuffer[V] = {
    group1 ++= group2
    group1
  }
  private def combineGroup(group: ArrayBuffer[V]): C = {
    var combiner : Option[C] = None
    group.foreach { v =>
      combiner match {
        case None => combiner = Some(createCombiner(v))
        case Some(c) => combiner = Some(mergeValue(c, v))
      }
    }
    combiner.get
  }
}

/**
 * An append-only map that spills sorted content to disk when the memory threshold
 * is exceeded. A group with type M is an intermediate combiner, and shares the same
 * type as either C or ArrayBuffer[V].
 */
class SpillableAppendOnlyMap[K, V, M, C](
    createGroup: V => M,
    mergeValue: (M, V) => M,
    mergeGroups: (M, M) => M,
    createCombiner: M => C,
    memoryThresholdMB: Long = 1024)
  extends Iterable[(K, C)] with Serializable {

  var currentMap = new AppendOnlyMap[K, M]
  var sizeTracker = new SamplingSizeTracker(currentMap)
  var oldMaps = new ArrayBuffer[DiskIterator]

  def insert(key: K, value: V): Unit = {
    def update(hadVal: Boolean, oldVal: M): M = {
      if (hadVal) mergeValue(oldVal, value) else createGroup(value)
    }
    currentMap.changeValue(key, update)
    sizeTracker.updateMade()
    if (sizeTracker.estimateSize() > memoryThresholdMB * 1024 * 1024) {
      spill()
    }
  }

  def spill(): Unit = {
    val file = File.createTempFile("external_append_only_map", "")  // Add spill location
    val out = new ObjectOutputStream(new FileOutputStream(file))
    val sortedMap = currentMap.iterator.toList.sortBy(kv => kv._1.hashCode())
    sortedMap.foreach(out.writeObject)
    out.close()
    currentMap = new AppendOnlyMap[K, M]
    sizeTracker = new SamplingSizeTracker(currentMap)
    oldMaps.append(new DiskIterator(file))
  }

  override def iterator: Iterator[(K, C)] = new ExternalIterator()

  // An iterator that sort-merges (K, M) pairs from memory and disk into (K, C) pairs
  class ExternalIterator extends Iterator[(K, C)] {

    // Order by key hash value
    val pq = PriorityQueue[KMITuple]()(Ordering.by(_.key.hashCode()))
    val inputStreams = Seq(new MemoryIterator(currentMap)) ++ oldMaps
    inputStreams.foreach(readFromIterator)

    // Read from the given iterator until a key of different hash is retrieved
    def readFromIterator(iter: Iterator[(K, M)]): Unit = {
      var minHash : Option[Int] = None
      while (iter.hasNext) {
        val (k, m) = iter.next()
        pq.enqueue(KMITuple(k, m, iter))
        minHash match {
          case None => minHash = Some(k.hashCode())
          case Some(expectedHash) if k.hashCode() != expectedHash => return
        }
      }
    }

    override def hasNext: Boolean = !pq.isEmpty

    override def next(): (K, C) = {
      val minKMI = pq.dequeue()
      var (minKey, minGroup) = (minKMI.key, minKMI.group)
      val minHash = minKey.hashCode()
      readFromIterator(minKMI.iterator)

      // Merge groups with the same key into minGroup
      var collidedKMI = ArrayBuffer[KMITuple]()
      while (!pq.isEmpty && pq.head.key.hashCode() == minHash) {
        val newKMI = pq.dequeue()
        if (newKMI.key == minKey) {
          minGroup = mergeGroups(minGroup, newKMI.group)
          readFromIterator(newKMI.iterator)
        } else {
          // Collision
          collidedKMI += newKMI
        }
      }
      collidedKMI.foreach(pq.enqueue(_))
      (minKey, createCombiner(minGroup))
    }

    case class KMITuple(key: K, group: M, iterator: Iterator[(K, M)])
  }

  // Iterate through (K, M) pairs in sorted order from the in-memory map
  class MemoryIterator(map: AppendOnlyMap[K, M]) extends Iterator[(K, M)] {
    val sortedMap = currentMap.iterator.toList.sortBy(km => km._1.hashCode())
    val it = sortedMap.iterator
    override def hasNext: Boolean = it.hasNext
    override def next(): (K, M) = it.next()
  }

  // Iterate through (K, M) pairs in sorted order from an on-disk map
  class DiskIterator(file: File) extends Iterator[(K, M)] {
    val in = new ObjectInputStream(new FileInputStream(file))
    var nextItem: Option[(K, M)] = None

    override def hasNext: Boolean = {
      nextItem = try {
        Some(in.readObject().asInstanceOf[(K, M)])
      } catch {
        case e: EOFException => None
      }
      nextItem.isDefined
    }

    override def next(): (K, M) = {
      nextItem match {
        case Some(item) => item
        case None => throw new NoSuchElementException
      }
    }
  }
}
