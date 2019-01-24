package spark_streaming_example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(20))

    // test running a Netcat server by `nc -lk 9999`
    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()

    ssc.awaitTermination()


  }


}
