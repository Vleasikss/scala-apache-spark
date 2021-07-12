package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

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

  }
}
