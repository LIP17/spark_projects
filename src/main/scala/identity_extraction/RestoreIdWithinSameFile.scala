package identity_extraction

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame
import Utils._

object RestoreIdWithinSameFile {

  private final val APP_NAME = "Restore Id From Single Sheet"
  private final val NUM_OF_INSTANCES = 2

  def main(args: Array[String]): Unit = {
    val session = createLocalSparkSession(APP_NAME, NUM_OF_INSTANCES)
    val rawData = readCSV(session, "/home/lip/Downloads/data/人社1_参保个人基本信息.csv")
    val idRestoredWithSelectedFields = query(rawData)
    writeCSV("/home/lip/Downloads/data/人员信息复原身份证号", idRestoredWithSelectedFields)
  }

  // 生成姓名，身份证号，社保号，生日，加密身份证号
  private def query(dataFrame: DataFrame): DataFrame = {
    dataFrame.select("姓名", "身份证号", "社会保障卡号", "出生日期")
      .withColumn("复原身份证号", replaceHiddenyyDigitUdf(col("身份证号"), col("出生日期")))
  }
}
