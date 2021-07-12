package org.example

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}


object Main {

  val APP_NAME = "app-name"
  val MASTER = "local[*]"
  val CSV_FILE_PATH = "data.csv"

  //  val conf: SparkConf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER)
  //  val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName(APP_NAME)
      .getOrCreate()


    val csvSchema = StructType(Array(
      StructField("id", DataTypes.IntegerType),
      StructField("firstname", DataTypes.StringType, false),
      StructField("lastname", DataTypes.StringType),
      StructField("email", DataTypes.BooleanType),
      StructField("email2", DataTypes.StringType),
      StructField("profession", DataTypes.StringType)))

    val df = session.read
      .option("delimiter", ",") // use comma delimiter
      .option("header", "true") // first line of csv is table-headers
      .option("inferSchema", "true") // automatically parse data-types
      .schema(csvSchema)
      .csv(CSV_FILE_PATH)
    df.printSchema()
    df.show()
  }
}
