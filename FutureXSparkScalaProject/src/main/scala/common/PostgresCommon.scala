package common

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object PostgresCommon {
  def PostgresCommonProp() : Properties = {

    println("getPostgresCommonProps method started")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user","postgres")
    pgConnectionProperties.put("password","admin")
    println("getPostgresCommonProps method ended")

    pgConnectionProperties

  }

  def getPostgresServerDatabase() : String = {
    val pgURL = "jdbc:postgresql://localhost:5432/futurex"

    pgURL
  }

  def fetchDataFrameFromPgTable(spark : SparkSession, pgTable : String) : DataFrame = {
    println("Fetching dataframe start")
    val pgCourseDataframe = spark.read.jdbc(getPostgresServerDatabase(),pgTable,PostgresCommonProp())
    println("Fetching dataframe ended")
    pgCourseDataframe.show()
    println("Dataframe shown")

    pgCourseDataframe
  }
}
