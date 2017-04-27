package com.cloudera.sa.bsci.spark2

import java.util.Calendar

import com.cloudera.sa.bsci.matmas.SegmentLoader
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by gmedasani on 4/25/17.
 */
/*
Driver class for Spark batch application
 */
object SAPIdocImportDriver {

  def main(args: Array[String]) {

    if(args.length < 2){
      System.err.println("Usage: SAPIdocImportDriver <inputDirectory> <outputDirectory>")
      System.exit(1)
    }

    val inputDirectory = args(0)
    val outputDirectory = args(1)
    val kuduMaster = "bos-1.gce.cloudera.com"
    val databaseName = "bos_poc"

    val sparkConf = new SparkConf().setAppName("SAPPlumber-IDocImport-MaterialMaster")
    //  .setMaster("local[2]") //Uncomment this line to test while developing on a workstation

    val sc = new SparkContext(sparkConf)
    val idocJSONRDD = sc.textFile(inputDirectory,3) //Read input
    val startTime = Calendar.getInstance().getTimeInMillis//get load start time

    //Load mara, marc, mard, mkal, marm, mean, mlgn, mlgt, makt, mbew, mlan, mvke and rawmarastaging tables
    idocJSONRDD.foreach(idocMessage => SegmentLoader.loadSegments(idocMessage,kuduMaster,databaseName))

    val endTime = Calendar.getInstance().getTimeInMillis//get load end time
    println("Parsing and Loading Kudu tables took: " + (endTime-startTime) + " seconds")

  }

}
