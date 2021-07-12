package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  val APP_NAME = "app-name"
  val MASTER = "local[*]"

  val conf: SparkConf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER)
  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val inputData = List[Double](35.5, 12.42341, 90.32)
    val rdd:RDD[Double] = sc.parallelize(inputData)

    val res = rdd.reduce((val1, val2) => val1 + val2)
    println(res) // 138.24340999999998
  }
}
