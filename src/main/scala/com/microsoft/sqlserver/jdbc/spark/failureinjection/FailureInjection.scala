/**
* Copyright 2020 and onwards Microsoft Corporation
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
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