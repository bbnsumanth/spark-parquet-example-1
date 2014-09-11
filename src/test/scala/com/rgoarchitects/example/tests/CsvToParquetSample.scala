package com.rgoarchitects.example.tests

import java.io.File


import com.rgoarchitects.example.Aggregates.Aggregate
import com.rgoarchitects.example.{Call, CalcAggregations}

import com.google.common.io.Files
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import parquet.hadoop.ParquetOutputFormat
import parquet.proto.{ProtoParquetOutputFormat, ProtoWriteSupport}

import org.scalatest.FunSpec


/**
 * User: arnonrgo
 * Date: 9/5/14
 * Time: 9:46 PM
 */
class CsvToParquetSample extends FunSpec{

  it("should aggregate into a parquet file") {


    val conf = new SparkConf(false)
      .setMaster("local[1]")
      .setAppName(" Spark processing")


    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    println(outputDir)


    val sc = new SparkContext(conf)


    val data = sc.textFile("src/test/resources/*.csv")


    val callPairs = data.map(l => {val c = Call(l)
      (c.getHourly,c)})


    val grouped = callPairs.groupByKey()
    val aggregates =grouped.values.map(g => (null,CalcAggregations(g)))

    val job = new Job()

    ParquetOutputFormat.setWriteSupportClass(job,classOf[ProtoWriteSupport[Aggregate]])
    ProtoParquetOutputFormat.setProtobufClass(job,classOf[Aggregate])
    aggregates.saveAsNewAPIHadoopFile(outputDir,classOf[Void],classOf[Aggregate],classOf[ParquetOutputFormat[Aggregate]],job.getConfiguration)

  }


}
