package org.apache.spark.util

import org.apache.spark.util.SamplingSizeTracker.Sample

/**
 * Estimates the size of an object as it grows, in bytes.
 * We sample with a slow exponential back-off using the SizeEstimator to amortize the time,
 * as each call to SizeEstimator can take a sizable amount of time (order of a few milliseconds).
 *
 * Users should call updateMade() every time their object is updated with new data, or 
 * flushSamples() if there is a non-linear change in object size (otherwise linear is assumed).
 * Not threadsafe.
 */
class SamplingSizeTracker(obj: AnyRef) {
  /**
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  private var lastLastSample: Sample = _
  private var lastSample: Sample = _

  private var numUpdates: Long = _
  private var nextSampleNum: Long = _

  flushSamples()

  /** Called after a non-linear change in the tracked object. Takes a new sample. */
  def flushSamples() {
    numUpdates = 0
    nextSampleNum = 1
    // Throw out both prior samples to avoid overestimating delta.
    lastSample = Sample(SizeEstimator.estimate(obj), 0)
    lastLastSample = lastSample
  }

  /** To be called after an update to the tracked object. Amortized O(1) time. */
  def updateMade() {
    numUpdates += 1
    if (nextSampleNum == numUpdates) {
      lastLastSample = lastSample
      lastSample = Sample(SizeEstimator.estimate(obj), numUpdates)
      nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
    }
  }

  /** Estimates the current size of the tracked object. O(1) time. */
  def estimateSize(): Long = {
    val interpolatedDelta =
      if (lastLastSample != null && lastLastSample != lastSample) {
        (lastSample.size - lastLastSample.size).toDouble /
          (lastSample.numUpdates - lastLastSample.numUpdates)
      } else if (lastSample.numUpdates > 0) {
        lastSample.size.toDouble / lastSample.numUpdates
      } else {
        0
      }
    val extrapolatedDelta = interpolatedDelta * (numUpdates - lastSample.numUpdates)
    val estimate = lastSample.size + extrapolatedDelta
    math.max(0, estimate).toLong
  }
}

object SamplingSizeTracker {
  case class Sample(size: Long, numUpdates: Long)
}