/*
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.sqlserver.jdbc.spark.failureinjection

import com.microsoft.sqlserver.jdbc.spark.SQLServerBulkJdbcOptions

import org.apache.spark.internal.Logging

/**
 * Failure injection to randomly restart the executors.
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

      val nextRandom = r.nextFloat()
      logDebug(s"FailureInjection: Generated random number is $nextRandom")
      if (nextRandom <= simulatedFailureRate) {
        logDebug("FailureInjection: Forced a crash")
        throw new Exception("FailureInjection forced a crash")
      }

      logDebug("FailureInjection: No failure injected")
    }
  }
}
