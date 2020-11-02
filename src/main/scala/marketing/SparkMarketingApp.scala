package marketing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class EventInfo(userId: String, eventType: String,
                     campaignId: String, channelId: String, purchaseId: String)

object SparkMarketingApp {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("marketing")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", 50)
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val testSchema = StructType(
      StructField("userId", StringType) ::
        StructField("eventId", StringType) ::
        StructField("eventTime", TimestampType) ::
        StructField("eventType", StringType) ::
        StructField("attributes", StringType) :: Nil
    )

    val testDF = spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(testSchema)
      .csv("file:///Users/mtoporova/Documents/csvData/mobile-app-clickstream_sample.csv")

    val neededEvents = Seq("app_open", "app_close", "purchase")

    val dfformatted = testDF.filter(r => neededEvents.contains(r.getAs[String]("eventType")))
      .withColumn("attrs",
      regexp_replace($"attributes", "[\\“\\”]", "\""))

    val mapSchema = MapType(StringType, StringType)
    val dfWithAttrs = dfformatted
      .withColumn("jsonAttrs",
        from_json(regexp_extract($"attrs", "\\{(.*)\\}", 1), mapSchema))
      .select("userId", "eventId", "eventTime", "eventType",
        "jsonAttrs.campaign_id", "jsonAttrs.channel_id", "jsonAttrs.purchase_id")

    val sessAgg = SessionInfoAggregator.toColumn.name("session")

    val df = dfWithAttrs.select($"userId", $"eventId", $"eventType",
      $"campaign_id", $"channel_id", $"purchase_id")
      .map { r =>
        EventInfo(
          r.getAs[String]("userId"),
          r.getAs[String]("eventType"),
          r.getAs[String]("campaign_id"),
          r.getAs[String]("channel_id"),
          r.getAs[String]("purchase_id"))
      }.groupByKey(ei => ei.userId).agg(sessAgg)

    val df1 = df.select($"key".as("userId"), explode($"session").as("sessionInfo"))
      .select($"userId", $"sessionInfo.campaignId", $"sessionInfo.channelId", $"sessionInfo.purchase")

    val df2 = df1
      .withColumn("sessionId",
        row_number.over(Window.partitionBy(lit(1)).orderBy("userId")))
      .withColumn("purchaseId", split($"purchase", ","))
      .select($"sessionId", $"campaignId", $"channelId", explode_outer($"purchaseId").as("purchaseId"))
      .persist()


    val purchasesSchema = StructType(
      StructField("purchaseId", StringType) ::
        StructField("purchaseTime", TimestampType) ::
        StructField("billingCost", DoubleType) ::
        StructField("isConfirmed", BooleanType) :: Nil
    )

    val purdf = spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .schema(purchasesSchema)
      .csv("file:///Users/mtoporova/Documents/csvData/purchases_sample - purchases_sample.csv")


    val findf = purdf.join(df2, purdf("purchaseId") <=> df2("purchaseId"), "full")
      .drop(df2("purchaseId")).persist()

    findf.createOrReplaceTempView("purchases")

    // What are the Top 10 marketing campaigns that bring
    // the biggest revenue (based on billingCost of confirmed purchases)?

    val q1query =
      s"""
         |select campaignId, sum(billingCost) as revenue
         |from purchases
         |where isConfirmed = true
         |group by campaignId
         |order by sum(billingCost) desc
         |limit 10
         |""".stripMargin

    val q1df = spark.sql(q1query)
    q1df.show(false)

    // What is the most popular (i.e. Top) channel that drives
    // the highest amount of unique sessions (engagements)  with the App in each campaign?

    val q2query =
      s"""
         |select campaignId, first(channelId) as topChannel from
         |     (select campaignId, channelId
         |      from purchases
         |      group by campaignId, channelId
         |      order by count(distinct sessionId) desc)
         |group by campaignId
         |""".stripMargin


    val q2df = spark.sql(q2query)

    q2df.show(false)

    findf.show(false)

    spark.stop()
  }
}
