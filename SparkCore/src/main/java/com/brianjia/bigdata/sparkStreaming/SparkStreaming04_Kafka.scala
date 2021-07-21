package com.brianjia.bigdata.sparkStreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object SparkStreaming04_Kafka {
  def main(args: Array[String]): Unit = {
    //TODO create env
    //StreamingContext: 1. sparkContextConf 2. batch gathering duration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "",
      ConsumerConfig.GROUP_ID_CONFIG -> "",
      "key.deserializer"-> "org.apache.kafka.common.serialization.StringDeserialization",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // TODO business logic
    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("atguigu"), kafkaPara)
    )

    kafkaDataDS.map(_.value()).print()

    //TODO close env
    //spark streaming has to be run continuously and listen to the required port
    //if the main method has been executed and the main program cannot be completed

    //1. start the gatherer
    ssc.start()

    //2. wait for gathering process to finish
    ssc.awaitTermination()
  }

}
