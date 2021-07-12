package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Main {

  val APP_NAME = "app-name"
  val MASTER = "local[*]"

  val conf: SparkConf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER)
  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.WARN)

    val inputData = List[Int](35, 12, 90)
    val rdd:RDD[Int] = sc.parallelize(inputData)


    val res = rdd.reduce((val1, val2) => val1 + val2)
    println(res) // 137

    val resMap = rdd.map(value => scala.math.sqrt(value))
    println(resMap)
    resMap.foreach(println)
  }
}
