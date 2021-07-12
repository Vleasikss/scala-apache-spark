package org.example

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}


object Main {

  val APP_NAME = "app-name"
  val MASTER = "local[*]"
  val CSV_FILE_PATH = "data.csv"
  val JSON_FILE_PATH = "data.json"

  //  val conf: SparkConf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER)
  //  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName(APP_NAME)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df: DataFrame = spark.read
      .option("inferSchema", "true") // Automatically infer data types & column names
      .option("multiline", "true")
      .json(JSON_FILE_PATH)

    df.printSchema()
    df.show()

  }
}
