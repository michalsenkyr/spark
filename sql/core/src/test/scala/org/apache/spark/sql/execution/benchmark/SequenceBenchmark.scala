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

package org.apache.spark.sql.execution.benchmark

import scala.reflect.ClassTag

/**
 * Benchmark [[Seq]], [[List]] and [[scala.collection.mutable.Queue]] serialization
 * performance.
 * To run this:
 *  1. replace ignore(...) with test(...)
 *  2. build/sbt "sql/test-only *benchmark.SequenceBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class SequenceBenchmark extends BenchmarkBase {
  val rows = 100
  val size = 10000

  import sparkSession.implicits._

  val sc = sparkSession.sparkContext

  def generate[T <: Seq[Int]](generator: Int => ( => Int) => T): Seq[T] = {
    Seq.fill(rows)(generator(size)(1))
  }

  test("Serialize and deserialize Seq") {
    val ds = sc.parallelize(generate(Seq.fill(_)), 1).toDS()
    ds.count() // force to build dataset
    runBenchmark("Serialize and deserialize Seq", rows) {
      ds.map(identity).collect()
    }
  }

  test("Serialize and deserialize List") {
    val ds = sc.parallelize(generate(List.fill(_)), 1).toDS()
    ds.count() // force to build dataset
    runBenchmark("Serialize and deserialize List", rows) {
      ds.map(identity).collect()
    }
  }

  test("Serialize and deserialize mutable.Queue") {
    val ds = sc.parallelize(generate(scala.collection.mutable.Queue.fill(_)), 1).toDS()
    ds.count() // force to build dataset
    runBenchmark("Serialize and deserialize mutable.Queue", rows) {
      ds.map(identity).collect()
    }
  }
}
