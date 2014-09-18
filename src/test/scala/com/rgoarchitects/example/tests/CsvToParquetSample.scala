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
    val tempDir2 = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val outputDir2 = new File(tempDir2, "output").getAbsolutePath
    println(outputDir)
    println(outputDir2)


    val sc = new SparkContext(conf)


    val data = sc.textFile("src/test/resources/*.csv")

    val calls = data.map (Call(_)).cache()

    val hourlyPairs = calls.map(c => (c.getHourly,c))
    val dailyPairs = calls.map(c => (c.getDaily,c))

    val groupedHourly = hourlyPairs.groupByKey()
    val groupedDaily = dailyPairs.groupByKey()

    val hourlyAggregates = groupedHourly.values.map(g => (null,CalcAggregations(g)))
    val dailyAggregates = groupedDaily.values.map(g => (null,CalcAggregations(g)))

    val job = new Job()

    ParquetOutputFormat.setWriteSupportClass(job,classOf[ProtoWriteSupport[Aggregate]])
    ProtoParquetOutputFormat.setProtobufClass(job,classOf[Aggregate])
    hourlyAggregates.saveAsNewAPIHadoopFile(outputDir,classOf[Void],classOf[Aggregate],classOf[ParquetOutputFormat[Aggregate]],job.getConfiguration)
    dailyAggregates.saveAsNewAPIHadoopFile(outputDir2,classOf[Void],classOf[Aggregate],classOf[ParquetOutputFormat[Aggregate]],job.getConfiguration)

  }


}
