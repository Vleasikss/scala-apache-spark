package org.example

import org.apache.spark.sql.{Column, SparkSession, functions}

/**
 * Dataset - is just a collection of rows
 */
object Main {

  val MASTER = "local[*]"
  val APP_NAME = "app-name"
  val CSV_FILE_PATH = "data.csv"
  val JSON_FILE_PATH = "data.json"


  val sqlDistinctId = "SELECT distinct(id) FROM USERS"
  val sqlMaxId = "SELECT max(id) FROM USERS"
  val sqlAvgId = "SELECT avg(id) FROM USERS"

  /**
   * +--------------+--------------------+---------+
   * |    profession|                 ids|ids_count|
   * +--------------+--------------------+---------+
   * |     developer|[108, 120, 113, 1...|        6|
   * |   firefighter|[111, 123, 146, 1...|       11|
   * ...
   */
  val sqlGroupByProfession = "SELECT profession, collect_set(id) as ids, count(id) as ids_count FROM USERS GROUP BY profession"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(MASTER)
      .appName(APP_NAME)
      .getOrCreate()


    val dataset = spark.read
      .option("header", "true")
      .csv(CSV_FILE_PATH)

    dataset.createOrReplaceTempView("users")
    spark.sql(sqlGroupByProfession).show(false)



    //    dataset.filter("id = '100' OR id = '101'").show()
    //    dataset.filter(row => row.getAs[String]("id") == "100").show()
    //    val id:Column = dataset.col("id")
    //    val id:Column = functions.col("id")
    //    dataset.filter(id.equalTo(100).or(id.equalTo(101))).show()


    //    val firstRow = dataset.first()
    //    val id = firstRow.getString(0)
    //    println(s"id of the first row is $id")
    //
    //    val numberOfRows = dataset.count()
    //    println(s"There are $numberOfRows records")
    spark.close()
  }
}
