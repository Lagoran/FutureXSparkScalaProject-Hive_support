import common.{PostgresCommon, SparkCommon}
import org.apache.log4j.spi.LoggerFactory
import org.apache.spark.sql.SparkSession

import java.util.Properties

object FutureXSparkTransformer {

  //Adding professional logging using the log4j.properties file
  private val Logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    Logger.info("Main method started")
    //This row below is the replacement for SparkCommon.scala
    val spark : SparkSession = SparkCommon.createSparkSession()

    //create a dataframe from Postgres Course catalog table.
    println("Creating dataframe from PG")

    val pgTable = "futurexschema.futurex_course_catalog"

    //This row below is the replacement for SparkCommon.scala
    val pgCourseDataFrame = PostgresCommon.fetchDataFrameFromPgTable(spark,pgTable)

  }
}
