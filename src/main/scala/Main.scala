package org.example

import org.apache.spark.sql.SparkSession

object Main {

  val MASTER = "local[*]"
  val APP_NAME = "app-name"
  val CSV_FILE_PATH = "data.csv"
  val JSON_FILE_PATH = "data.json"

  val tableName = "user"
  val user = "root"
  val pass = "root"
  val database = "apache_spark_example"
  val jdbcUrl = s"jdbc:mysql://localhost:3306/$database"
  val jdbcDriver = "com.mysql.cj.jdbc.Driver"


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(MASTER)
      .appName(APP_NAME)
      .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 8)

    val jdbcDF = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", pass)
      .option("driver", jdbcDriver)
      .load()

    jdbcDF.show()
    jdbcDF.printSchema()
  }
}

