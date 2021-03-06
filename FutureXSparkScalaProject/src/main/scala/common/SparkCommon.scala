package common

import common.PostgresCommon.getClass
import org.apache.log4j.spi.LoggerFactory
import org.apache.spark.sql.SparkSession

object SparkCommon {

  private val Logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession(): SparkSession = {
    // Create a Spark Session
    // For Windows

    println("Create Spark Session method started")

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    // .config("spark.sql.warehouse.dir",warehouseLocation).enableHiveSupport()

    val spark = SparkSession
      .builder
      .appName("HelloSpark")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    println("Created Spark Session ended")

    spark

    //>>Test section with dummy dataframe creation
    // val sampleSeq = Seq((1,"spark"),(2,"Big Data"))

    //val df = spark.createDataFrame(sampleSeq).toDF("course id", "course name")
    //df.show()
    //df.write.format("csv").save("samplesq")
    //<<

  }
}
