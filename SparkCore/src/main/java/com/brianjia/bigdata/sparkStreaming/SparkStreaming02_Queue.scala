package com.brianjia.bigdata.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_Queue {
  def main(args: Array[String]): Unit = {
    //TODO create env
    //StreamingContext: 1. sparkContextConf 2. batch gathering duration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //TODO business logic
    //get socket data
    val rddQueue = new mutable.Queue[RDD[Int]]()
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)
    val reducedStream: DStream[(Int, Int)] = inputStream.map((_, 1)).reduceByKey(_ + _)
    reducedStream.print()

    //TODO close env
    //spark streaming has to be run continuously and listen to the required port
    //if the main method has been executed and the main program cannot be completed
//    ssc.stop()
    //1. start the gatherer
    ssc.start()

    for ( i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    //2. wait for gathering process to finish
    ssc.awaitTermination()
  }

}
