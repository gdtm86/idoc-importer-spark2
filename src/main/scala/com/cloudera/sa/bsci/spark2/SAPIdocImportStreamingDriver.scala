package com.cloudera.sa.bsci.spark2

import com.cloudera.sa.bsci.matmas.SegmentLoader
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gmedasani on 4/25/17.
 */

/*
Driver class for SparkStreaming application
 */
object SAPIdocImportStreamingDriver {

  def main (args: Array[String]) {
    if(args.length < 9){
      System.err.println("Usage: SAPIdocImportStreamingDriver <batch-duration-in-seconds> <kafka-bootstrap-servers> " +
        "<kafka-topics> <kafka-consumer-group-id> <kafka-security.protocol> <kafka-ssl-truststore-location> " +
        "<kafka-ssl-truststore-password> <kudu-master-address> <kudu-database-name>")
      System.exit(1)
    }

    val batchDuration = args(0)
    val bootstrapServers = args(1).toString
    val topicsSet= args(2).toString.split(",").toSet
    val consumerGroupID = args(3)
    val kafkaSecurityProtol = args(4)
    val sslTruststoreLocation = args(5)
    val sslTruststorePassword = args(6)
    val kuduMaster = args(7)
    val databaseName = args(8)


    val sparkConf = new SparkConf().setAppName("Streaming-SAPPlumber-IdocImport-MATMAS")
    //          .setMaster("local[4]") //Uncomment this line to test while developing on a workstation
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(batchDuration.toLong))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> consumerGroupID,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol" -> kafkaSecurityProtol,
      "ssl.truststore.location" -> sslTruststoreLocation,
      "ssl.truststore.password" -> sslTruststorePassword,
      "sasl.kerberos.service.name" -> "kafka"
    ) //Define various Kafka params required to create a consumer including security features

    val topics = topicsSet.toArray

    val inputDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )//Create a Kafka DStream from SparkStreaming Context and KafkaParams

    val inputDStreamMessages = inputDStream.map(record => record.value())//Get only kafka message body. Ignore keys

    /*
    For each RDD in a DStream apply a map transformation that loads segments into Kudu.
    Potential Improvements: Try to use mapPartitions to reduce number of connections to Kudu servers
     */
    inputDStreamMessages.foreachRDD(rdd => {
      val rdd1 = rdd.map(idocMessage => SegmentLoader.loadSegments(idocMessage,kuduMaster,databaseName))
      println("I'm here")
      rdd1.foreach(println)
    })

    println("Number of messages processed" + inputDStreamMessages.count())

    ssc.start()
    ssc.awaitTermination()

  }

}
