package com.sensetime.wk

import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappySession, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by sensetime on 17-6-12.
  */
object SnappyByteTest {

  case class OPQObject(uuid: String, opq: Array[Byte])

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("lack of filename ")
      System.exit(1)
    }
    val spark = SparkSession
      .builder()
      .appName("Snappy Byte Serialize Test")
      .getOrCreate()

    val snappy = new SnappySession(spark.sparkContext)
    snappy.sql("drop table if exists opq_table")
    snappy.sql("create table opq_table (uuid varchar(64) not null, opq blob not null) using column")

    //text data locate: /home/sensetime/IdeaProjects/learning-demo/data.txt
    import snappy.implicits._
    var startTime = System.currentTimeMillis()
    val textFile = snappy.read.textFile(args(0))
    val opqDBRDD = textFile.map(_.split(",")).map(p => OPQObject(p(0), Base64.decodeBase64(p(1).getBytes()))).toDF()
    opqDBRDD.write.insertInto("opq_table")
    val readStartTime=System.currentTimeMillis()
    //for(i<-1 to 0){
      val opqs = snappy.sql("select opq from opq_table").map(o=>o.getAs[Array[Byte]](0))
      opqs.collect()
    //}
    var endTime = System.currentTimeMillis()
    println("time used(use snappyData): " + (endTime - startTime))
    println("read data from snappy time is: "+(endTime-readStartTime))

    //清理JVM
    textFile.unpersist()
    System.gc()
    System.runFinalization()

    startTime=System.currentTimeMillis()

//    for(i<-1 to 10){
//      val textFile1= spark.read.textFile(args(0))
//      val opqDBRDD1 = textFile1.map(_.split(",")).map(p=>Base64.decodeBase64(p(1).getBytes()))
//      opqDBRDD1.collect()
//      textFile1.unpersist()
//    }
//    endTime=System.currentTimeMillis()
//    println("time used(use directly): "+(endTime-startTime))
  }
}
