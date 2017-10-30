package learn_spark_ch2

import org.apache.spark.sql.SparkSession


object SimpleApp {

  def main(args: Array[String]): Unit = {

    val logFile = "/home/lip/Documents/spark-2.2.0-bin-hadoop2.7/README.md"

    val spark = SparkSession.builder().appName("simple app").getOrCreate()

    val logData = spark.read.textFile(logFile)

    val count = logData.count()

    println(count)

    spark.stop()
  }
}
