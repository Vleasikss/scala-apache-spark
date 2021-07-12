package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Main {

  val MASTER = "local[*]"
  val APP_NAME = "app-name"
  val CSV_FILE_PATH = "data.csv"
  val JSON_FILE_PATH = "data.json"


  // optional (exclude job working)
  val csvSchema: StructType = StructType(Array(
    StructField("id", DataTypes.IntegerType),
    StructField("firstname", DataTypes.StringType),
    StructField("lastname", DataTypes.StringType),
    StructField("email", DataTypes.BooleanType),
    StructField("email2", DataTypes.StringType),
    StructField("profession", DataTypes.StringType)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(MASTER)
      .appName(APP_NAME)
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("delimiter", ",")
      .option("inferSchema", "true") // automatically recognize data types
      .schema(csvSchema)
      .csv(CSV_FILE_PATH)

    df.printSchema()
    df.show()

  }
}
