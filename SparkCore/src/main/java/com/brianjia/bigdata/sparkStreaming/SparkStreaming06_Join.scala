package com.brianjia.bigdata.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_Join {
  def main(args: Array[String]): Unit = {
    //TODO create env
    //StreamingContext: 1. sparkContextConf 2. batch gathering duration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val data9999: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val data8888: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

    val map9999: DStream[(String, Int)] = data9999.map((_, 9))
    val map8888: DStream[(String, Int)] = data8888.map((_, 8))

    val value: DStream[(String, (Int, Int))] = map9999.join(map8888)
    value.print()
    
    //TODO close env
    //spark streaming has to be run continuously and listen to the required port
    //if the main method has been executed and the main program cannot be completed

    //1. start the gatherer
    ssc.start()

    //2. wait for gathering process to finish
    ssc.awaitTermination()
  }

}
