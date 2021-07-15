package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import SomeData.{APP_NAME, MASTER, input1, input2}


object InnerJoin {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(MASTER)
      .appName(APP_NAME)
      .getOrCreate()

    val sc = spark.sparkContext
    val users = sc.parallelize(input1)
    val cities = sc.parallelize(input2)
    val result: RDD[(Int, (String, String))] = users.join(cities)
    result.foreach(println)
  }
}
