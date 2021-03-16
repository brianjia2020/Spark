package com.brianjia.bigdata.sparkCore.test

import java.io.ObjectInputStream
import java.net.{ServerSocket, Socket}

/**
 * Executor is the actual worker to do the computation
 * data and logic are sent from Driver
 *
 * In real practise, there can be many executors and many logics.
 * So real fun stuff will start from here....
 */
object Executor {
  def main(args: Array[String]): Unit = {
    //1. start the server
    val server = new ServerSocket(9999)
    //2. wait for client side connection
    val client: Socket = server.accept()
    val in = client.getInputStream

    val objIn = new ObjectInputStream(in)
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val res: List[Int] = task.compute()
    print("The result of task compute[9999] is: " + res)
    in.close()
    client.close()
    server.close()
  }
}
