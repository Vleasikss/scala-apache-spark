package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Results:
 *  - Аналогично как из CSV, при указании схемы, мы избегаем дополнительного задания (Job).
 *  - Через схему можем указать новый тип данных.
 *  - Схема может быть сложной по своей структуре.
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

    val df: DataFrame = spark.read
      .option("inferSchema", "true")
      .option("multiline", "true") // allows to use json files with tabulation/spaces
      .json(JSON_FILE_PATH)

    df.show()
    df.printSchema()

  }
}
