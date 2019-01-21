package identity_extraction

import org.apache.spark.sql.functions.col
import Utils._
import org.apache.spark.sql.DataFrame

object RestoreIdFromMultipleFiles {

  private final val APP_NAME = "Join Multiple Tables"
  private final val NUM_OF_INSTANCES = 2

  def main(args: Array[String]): Unit = {
    val session = createLocalSparkSession(APP_NAME, NUM_OF_INSTANCES)

    val personalInfo = readCSV(session, "/home/lip/Downloads/data/人社1_参保个人基本信息.csv")
    val feeInfo = readCSV(session, "/home/lip/Downloads/data/人社1_个人应缴实缴明细.csv")

    val result = query(personalInfo, feeInfo)

    writeCSV("/home/lip/Downloads/data/joined_sheets", result)
  }

  private def query(personalInfo: DataFrame, feeInfo: DataFrame): DataFrame = {
    personalInfo.select("姓名", "身份证号", "社会保障卡号", "出生日期", "参保单位编号", "参保个人编号")
      .join(feeInfo, Seq("参保单位编号", "参保个人编号"))
      .withColumn("复原身份证号", replaceHiddenyyDigitUdf(col("身份证号"), col("出生日期")))
  }
}
