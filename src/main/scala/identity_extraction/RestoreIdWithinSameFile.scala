package identity_extraction

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utils._

object RestoreIdWithinSameFile {

  private final val APP_NAME = "GET Identity"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val rawData = readCSV(session, "/home/lip/Downloads/data/人社1_参保个人基本信息.csv")
    val idRestoredWithSelectedFields = query(rawData)
    writeCSV("/home/lip/Downloads/data/人员信息复原身份证号", idRestoredWithSelectedFields)
  }

  val replaceHiddenDigitUdf = udf((id: String, dob: String) => replaceSixStarsWithShortendDate(id, dob))

  // 生成姓名，身份证号，社保号，生日，加密身份证号
  def query(dataFrame: DataFrame): DataFrame = {
    dataFrame.select("姓名", "身份证号", "社会保障卡号", "出生日期")
      .withColumn("复原身份证号", replaceHiddenDigitUdf(col("身份证号"), col("出生日期")))
  }

}
