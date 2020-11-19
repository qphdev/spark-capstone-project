package marketing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.StructType


object MarketingDataUtils extends SparkSessionWrapper {
  import spark.implicits._

  /**
   * Replaces quotes '“', '”' with default '"' in attributes column of clickstream DF
   */
  def normalizeQuotes(clickstreamDf: DataFrame): DataFrame = {
    clickstreamDf
      .withColumn("attrs", regexp_replace($"attributes", "[\\“\\”]", "\""))
      .drop($"attributes")
  }


  /**
   * Reads DataFrame with given schema from csv
   */
  def readCsvData(path: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(schema)
      .csv(path)
  }


  /**
   * Writes given DF to (one) csv file
   */
  def saveDfToCsv(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .format("csv")
      .option("header", true)
      .option("sep", ",")
      .mode("overwrite")
      .save(path)
  }
}
