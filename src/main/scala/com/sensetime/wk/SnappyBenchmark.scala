package com.sensetime.wk

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SnappySession, SparkSession}

/**
  * Created by sensetime on 17-6-10.
  */
object SnappyBenchmark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SnappyBenchmark")
      .getOrCreate()

    //Create a DataFrame and temp table using Spark's range method
    var testDF = spark.range(100000000).selectExpr("id", "concat('sym', cast((id % 100) as STRING)) as sym")
    testDF.cache
    testDF.createOrReplaceTempView("sparkCacheTable")
    //Run a query and to check the performance
    benchmark("Spark perf") {spark.sql("select sym, avg(id) from sparkCacheTable group by sym").collect()}

    //clean up the JVM
    testDF.unpersist()
    System.gc()
    System.runFinalization()

    val snappy=new org.apache.spark.sql.SnappySession(spark.sparkContext)
    testDF = snappy.range(100000000).selectExpr("id", "concat('sym', cast((id % 100) as varchar(10))) as sym")
    snappy.sql("drop table if exists snappyTable")
    snappy.sql("create table snappyTable (id bigint not null, sym varchar(10) not null) using column")

    //Insert the created DataFrame into the table and measure its performance
    benchmark("Snappy insert perf", 1, 0) {testDF.write.insertInto("snappyTable") }
    benchmark("Snappy perf") {snappy.sql("select sym, avg(id) from snappyTable group by sym").collect()}

  }

  def benchmark(name: String, times: Int = 10, warmups: Int = 6)(f: => Unit) {
    for (i <- 1 to warmups) {
      f
    }
    val startTime = System.nanoTime
    for (i <- 1 to times) {
      f
    }
    val endTime = System.nanoTime
    println(s"Average time taken in $name for $times runs: " +
      (endTime - startTime).toDouble / (times * 1000000.0) + " millis")
  }
}
