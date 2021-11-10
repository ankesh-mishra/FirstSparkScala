package sparksql

import org.apache.spark.sql.SparkSession

object FirstJob {

  def main (args: Array[String]) =
    {

      val spark = SparkSession.builder().appName("First-App").master("local").getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("INFO")
      val df = spark.read.option("Header", "True").csv("TX417945_8515.csv")
      df.show()

      spark.stop()
    }
}
