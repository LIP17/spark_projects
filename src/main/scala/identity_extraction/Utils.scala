package identity_extraction

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Created by lip on 1/20/19.
  */
object Utils {

  /**
    * fill
    * @param toBeFilled string with 6 `*` inside
    * @param date: Date in YYYYMMDD format
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
}
