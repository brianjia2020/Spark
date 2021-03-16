package com.brianjia.bigdata.sparkCore.test

/**
 * Task is a sample of RDD
 * RDD is the data structure that Spark encapsulate.
 * subTask is where the driver splits the tasks and assign proper data and logic
 *
 * Nice!!!!
 */
class Task extends Serializable {
  val datas = List(1,2,3,4)
  val logic = (num: Int) => {
    num*2
  }
}
