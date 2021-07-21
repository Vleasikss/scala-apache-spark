package org.example

import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    val data: DataFrame = spark.read
      .option("header", "true")
      .csv(CSV_FILE_PATH)

    data.selectExpr("id / 2").show()
    data.select("id")

  }
}
