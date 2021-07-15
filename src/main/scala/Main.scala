package org.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * PairRdd<Any,Any>
 * Key Value
 */
object Main {

  val MASTER = "local[*]"
  val APP_NAME = "app-name"
  val CSV_FILE_PATH = "data.csv"
  val JSON_FILE_PATH = "data.json"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(MASTER)
      .appName(APP_NAME)
      .getOrCreate()

    val sc = spark.sparkContext

    val inputData: List[String] = List(
      "WARN: Tuesday 4 September 0405",
      "ERROR: Tuesday 4 September 0408",
      "FATAL: Wednesday 5 September 1632",
      "ERROR: Friday 7 September 1854",
      "WARN: Saturday 8 September 1942",
    )

    val rddLogs: RDD[(String, Long)] = sc.parallelize(inputData)
      .map(rawValue => (rawValue.split(": ")(0), 1L) )
      .reduceByKey((value1, value2) => value1 + value2)

    println(rddLogs.foreach(println))


  }
}
