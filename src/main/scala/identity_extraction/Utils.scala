package identity_extraction

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.functions.udf

object Utils {

  def createLocalSparkSession(appName: String, numOfInstances: Int): SparkSession = {
    val conf = new SparkConf().setAppName(appName).setMaster(s"local[$numOfInstances]")
    SparkSession.builder().config(conf).getOrCreate()
  }

  /**
    * fill the toBeFilled with 6 `*`s inside, fill them with shorten date
    * @param toBeFilled string with 6 `*`s inside
    * @param date: Date in YYYYMMDD format and will be saved as YYMMDD
    * */
  def replaceSixStarsWithShortendDate(toBeFilled: String, date: String): String = {
    if(toBeFilled == null || date == null) "Abnormal"
    else toBeFilled.replace("******", date.substring(2))
  }

  def readCSV(session: SparkSession, file: String): DataFrame = {
    session.read.format("csv")
      .option("header", "true")
      .load(file)
  }

  def writeCSV(file: String, dataFrame: DataFrame): Unit = {
    dataFrame.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(file)
  }

  val replaceHiddenyyDigitUdf = udf((id: String, dob: String) => replaceSixStarsWithShortendDate(id, dob))
}
