import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object example {

  val input = "data/client_entries.csv"
  val output = "output/client_entries_processed"

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkChargePoints")
    .getOrCreate()

  def extract(): DataFrame = {
    spark.read.option("header", "true").csv(input)
  }

  def transform(df: DataFrame): DataFrame = {
    val new_df = df.withColumn("Total_Entry", col("Total_Entry").cast(IntegerType)).
                    filter(df("market_direction") === "buy").
                    filter(df("currency") === "GBP").
                    groupBy("profile_id").
                    agg(sum("Total_Entry").as("Total(GBP)"))
    new_df
  }

  def load(df: DataFrame): Unit = {
    df.write.option("header", "true").csv(output)
  }

  def main(args: Array[String]): Unit = {
    load(transform(extract()))
  }
}