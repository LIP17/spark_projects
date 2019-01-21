package identity_extraction

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object GetIdentityIndex {

  private final val APP_NAME = "GET Identity"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(APP_NAME).setMaster("local[2]")
    val session = SparkSession.builder().config(conf).getOrCreate()
    val data = readCSV("/home/lip/Downloads/data/人社1_参保个人基本信息.csv", session)

    val idRestored = generateIndex(data)
    writeCSV("/home/lip/Downloads/data/人员信息复原身份证号", idRestored)
  }

  def replaceHiddenDigitsInId(id: String, dob: String): String = {
    if(id == null || dob == null) {
      "Abnormal"
    } else {
      id.replace("******", dob.substring(2))
    }
  }

  val replaceHiddenDigitUdf = udf((id: String, dob: String) => replaceHiddenDigitsInId(id, dob))

  // 生成姓名，身份证号，社保号，生日，加密身份证号
  def generateIndex(dataFrame: DataFrame): DataFrame = {

    dataFrame.select("姓名", "身份证号", "社会保障卡号", "出生日期")
      .withColumn("复原身份证号", replaceHiddenDigitUdf(col("身份证号"), col("出生日期")))
  }

  def readCSV(file: String, session: SparkSession): DataFrame = {
    session.read.format("csv")
      .option("header", "true")
      .load(file)
  }

  def writeCSV(file: String, dataFrame: DataFrame) = {
    dataFrame.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(file)
  }

}
