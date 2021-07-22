package com.brianjia.bigdata.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window1 {
  def main(args: Array[String]): Unit = {
    //TODO create env
    //StreamingContext: 1. sparkContextConf 2. batch gathering duration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne = lines.map((_, 1))

    wordToOne.reduceByKeyAndWindow(
      (x:Int,y:Int) => {x+y},
      (x:Int, y:Int) => {x-y},
      Seconds(9),Seconds(3)
    ).print()

    //TODO close env
    //spark streaming has to be run continuously and listen to the required port
    //if the main method has been executed and the main program cannot be completed

    //1. start the gatherer
    ssc.start()

    //2. wait for gathering process to finish
    ssc.awaitTermination()
  }

}
