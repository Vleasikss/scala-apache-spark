package org.example

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Main {

  val MASTER = "local[*]"
  val APP_NAME = "app-name"
  val CSV_FILE_PATH = "data.csv"
  val JSON_FILE_PATH = "data.json"


  /**
   *
   * @param df - dataframe
   * @return count of columns in dataframe
   */
  def getCountOfCols(df: DataFrame): Long = {
    df.count()
  }

  /**
   * Caches dataframe count of columns
   * (On each getting, it won't calculate the count age)
   *
   * @param df - dataFrame
   */
  def cacheCountOfCols(df: DataFrame): Unit = df.cache()
    .count()

  /**
   * Removes count of columns cache
   *
   * @param df - dataFrame
   */
  def unCacheCountOfCols(df: DataFrame): Unit = {
    df.unpersist().count()
  }

  /**
   *  1. Creates 'limit' transformation;
   *  1. Creates 'show' action (transformation won't be proceed if action is not defined)
   *
   * @param df    - dataFrame
   * @param limit - show limit
   * @return table with only first 'limit' columns
   */
  def showTableLimited(df: DataFrame, limit: Int): DataFrame = {
    val limitedDf: Dataset[Row] = df.limit(limit)
    limitedDf.show()
    limitedDf
  }

  /**
   *  1. Creates 'select' transformation;
   *  1. Creates 'show' action (transformation won't be proceed if action is not defined)
   *
   * @return table with only 'column'
   *
   */
  def selectColumn(df: DataFrame, column: String): DataFrame = {
    val selectedColumn = df.select(column)
    selectedColumn.show()
    selectedColumn
  }

  /**
   *
   * @param df     - dataFrame
   * @param column - column to delete
   * @return table with all the columns except 'column'
   */
  def deleteColumn(df: DataFrame, column: String): DataFrame = {
    val allTheColumnsExcept = df.drop(column)
    allTheColumnsExcept.show()
    allTheColumnsExcept
  }


  /**
   * There are two method to remove duplicates from table:
   *  1. using df.distinct()
   *  1. using df.dropDuplicates()
   *  1. using df.dropDuplicates(List(column1, column2, column3))
   *
   * @param df - dataFrame
   * @return table without duplicates
   */
  def distinctTable(df: DataFrame): DataFrame = {
    //    val distinct = df.distinct()
    val withoutSameId = df.dropDuplicates(List("id")) // drop duplicates by id
    withoutSameId
  }

  /**
   * creates temporary view to create sql queries
   *
   * @param spark - sparkSession
   * @param dataFrame - dataFrame
   * @param viewName - temporary sql table name
   * @param sqlQuery - sql query to be proceeded
   * @return results of proceeded sql query
   */
  def doSqlTransformation(spark: SparkSession, dataFrame: DataFrame, viewName: String, sqlQuery: String): DataFrame = {
    dataFrame.createOrReplaceTempView(viewName)
    val result: DataFrame = spark.sql(sqlQuery)
    result.show()
    result
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master(MASTER)
      .appName(APP_NAME)
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(CSV_FILE_PATH)
    //    df.show()
    //    df.printSchema()

    //    showTableLimited(df, 5)
    //    selectColumn(df, "id")
    deleteColumn(df, "id")
    distinctTable(df)

    val viewName = "temporaryTable"
    doSqlTransformation(spark, df, viewName, s"SELECT * FROM $viewName WHERE id > 140")
  }
}
