package com.brianjia.bigdata.sparkStreaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_State {
  def main(args: Array[String]): Unit = {
    //TODO create env
    //StreamingContext: 1. sparkContextConf 2. batch gathering duration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    val datas = ssc.socketTextStream("localhost",9999)
    //stateless transformation
    //however we often need to keep the aggregation result
    datas
      .map((_,1))
      .updateStateByKey(
        // same key, the incoming value
        // option is the in-mem value
        (seq: Seq[Int], buff: Option[Int]) => {
          val newCount = buff.getOrElse(0) + seq.sum
          Option(newCount)
        }
      )
      .print()

    //TODO close env
    //spark streaming has to be run continuously and listen to the required port
    //if the main method has been executed and the main program cannot be completed

    //1. start the gatherer
    ssc.start()

    //2. wait for gathering process to finish
    ssc.awaitTermination()
  }

}
