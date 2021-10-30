import common.{PostgresCommon, SparkCommon}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object FutureXSparkTransformer {
  def main(args: Array[String]): Unit = {

    //This row below is the replacement for SparkCommon.scala
    val spark : SparkSession = SparkCommon.createSparkSession()

    //create a dataframe from Postgres Course catalog table.
    println("Creating dataframe from PG")

    val pgTable = "futurexschema.futurex_course_catalog"

    //This row below is the replacement for SparkCommon.scala
    val pgCourseDataFrame = PostgresCommon.fetchDataFrameFromPgTable(spark,pgTable)

  }
}
