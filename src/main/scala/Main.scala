package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Main {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()
    val lines = spark.sparkContext.parallelize(
      Seq("Hello World\nBye world"))

    val counts = lines
      .flatMap(line => line.split("\n"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
  }
}
