package sparksql

import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.parquet.format.IntType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object FirstJob {

  def main (args: Array[String]) =
    {
      val sSchema = StructType(Array(
        StructField("DAY", StringType),
        StructField("JD", StringType),
        StructField("MON", StringType),
        StructField("SID", StringType),
        StructField("YR", StringType),
        StructField("PRCP", StringType),
        StructField("TAVG", StringType),
        StructField("TMAX", StringType)
      ))
      val spark = SparkSession.builder().appName("First-App").master("local").getOrCreate()
      import spark.implicits._
      spark.sparkContext.setLogLevel("INFO")
      val df = spark.read.option("Header", "True").schema(sSchema).csv("TX417945_8515.csv")
      // val df = spark.read.option("Header", "True").csv("TX417945_8515.csv")
      df.show()
      val df1 = df.filter("YR==1946")

      df1.show()

      // stop spark application
      spark.stop()
    }
}
