package org.example

import org.apache.hadoop.util.ShutdownHookManager
import org.apache.spark.sql.SparkSession.setActiveSession
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, not}

import java.util.Scanner

/**
 * Transformation are LAZY,
 *  - always return DataFrame
 *  - immutable
 *  - has wide and narrow operations
 *  - contains two types of casting: typed (return DaraFrame) && untyped (return DataSet[T])
 *    Actions are EAGER
 *  - always return result or write on disk
 *
 *
 * Wide vs Narrow transformations:
 *  - narrow transformation:
 *    - calculates for only partition that locates no more than one partition of parent RDD
 *    - for example:
 *      - filter(..)
 *      - drop(..)
 *      - coalesce(..)
 *  - wide transformation:
 *    - calculates for only partition that can be located in many partitions of parent RDD
 *    - for example:
 *      - distinct()
 *      - groupBy(..).sum()
 *      - repartition(n)
 *
 * use (mapToPair && reduceByKey) instead of groupByKey -> it works faster
 * reduceByKey contains two phases:
 *  - mapping partitions on its same keys
 *  - joining partitions keys  (shuffle (that works really fast))
 */
object Main extends App{

  val MASTER = "local[*]"
  val APP_NAME = "app-name"
  val CSV_FILE_PATH = "data.csv"
  val JSON_FILE_PATH = "data.json"

  val spark = SparkSession.builder
    .master(MASTER)
    .appName(APP_NAME)
    .getOrCreate()

  val read: Array[Row] = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(CSV_FILE_PATH)
    //      .schema(schema) // if not exists, a job will be called
    .where("id > 130") // transformation
    .drop("firstname") // transformation
    .filter(col("id").notEqual("149")) // transformation
    .filter(col("id").notEqual("138")) // transformation
    .filter(not(col("email").startsWith("Wendi"))) // transformation
    .take(10) // action


  // won't finish the localhost spark server
  val scanner = new Scanner(System.in)
  scanner.nextLine()

  //    read.show()
  //    spark.close()


}
