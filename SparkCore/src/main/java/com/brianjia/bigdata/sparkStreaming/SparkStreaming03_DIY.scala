package com.brianjia.bigdata.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaming03_DIY {
  def main(args: Array[String]): Unit = {
    //TODO create env
    //StreamingContext: 1. sparkContextConf 2. batch gathering duration
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // TODO business logic
    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    messageDS.print()
    //TODO close env
    //spark streaming has to be run continuously and listen to the required port
    //if the main method has been executed and the main program cannot be completed

    //1. start the gatherer
    ssc.start()

    //2. wait for gathering process to finish
    ssc.awaitTermination()
  }

  /**
   * self-defined data gatherer class
   * 1. extends Receiver
   * 2. implement the onStart and onStop method
   *
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag) {
            val message = "gathered message is: " + new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false;
    }
  }
}
