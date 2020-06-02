package com.microsoft.sqlserver.jdbc.spark

import com.microsoft.sqlserver.jdbc.spark.DataPoolUtils.logDebug
import org.apache.spark.internal.Logging

/**
 * Failure injection to to randomly restart the executors.
 */

object FailureInjection extends Logging {
  val simulatedFailureRate = 0.03 // fail 1 in 30 times
  val seed = System.nanoTime()
  val r = new scala.util.Random(seed)

  /**
   * simulateRandomRestart simulate random restarts of the calling process by throwing
   * an exception when testDataIdempotency is set as true by user.
   * @param options user specified option testDataIdempotency.
   */
  def simulateRandomRestart(options: SQLServerBulkJdbcOptions): Unit = {
    if (options.testDataIdempotency == true) {
      logDebug(s"FailureInjection: testDataIdempotency is set to True")
      logDebug(s"FailureInjection: seed used is $seed")

      val nextRandom = r. nextFloat()
      logDebug(s"FailureInjection: Generated random number is $nextRandom")
      if (nextRandom <= simulatedFailureRate) {
        logDebug("FailureInjection: Forced a crash")
        throw new Exception("FailureInjection forced a crash")
      }

      logDebug("FailureInjection: No failure injected")
    }
  }
}