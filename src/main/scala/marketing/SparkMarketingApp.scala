package marketing

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


case class EventInfo(userId: String, eventType: String,
                     campaignId: String, channelId: String, purchaseId: String)

object SparkMarketingApp {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("marketing")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val clickstreamSchema = StructType(
      StructField("userId", StringType) ::
        StructField("eventId", StringType) ::
        StructField("eventTime", TimestampType) ::
        StructField("eventType", StringType) ::
        StructField("attributes", StringType) :: Nil
    )

    val clickstreamDf = readCsvData("file:///Users/mtoporova/Documents/csvData/mobile-app-clickstream_sample.csv",
      clickstreamSchema)

    val clickstreamFiltered = filterNeededEvents(clickstreamDf, Seq("app_open", "app_close", "purchase"))
    val clickstreamQuotedAttrs = normalizeQuotesToNewCol(clickstreamFiltered)

    val clickstreamWithAttrs = extractAttributes(clickstreamQuotedAttrs)

    val clickstreamSessionsPerUser = aggClickstreamSessions(clickstreamWithAttrs)
    val clickstreamSessions = explodeClickstreamSessions(clickstreamSessionsPerUser)
    val clickstreamWithSessionIds = generateSessionIds(clickstreamSessions)
    val clickstreamSessionsPerPurchase = explodePurchases(clickstreamWithSessionIds)

    val purchasesSchema = StructType(
      StructField("purchaseId", StringType) ::
        StructField("purchaseTime", TimestampType) ::
        StructField("billingCost", DoubleType) ::
        StructField("isConfirmed", BooleanType) :: Nil
    )

    val purdf = readCsvData("file:///Users/mtoporova/Documents/csvData/purchases_sample - purchases_sample.csv",
      purchasesSchema)


    val findf = purdf
      .join(clickstreamSessionsPerPurchase,
        purdf("purchaseId") <=> clickstreamSessionsPerPurchase("purchaseId"), "full")
      .drop(clickstreamSessionsPerPurchase("purchaseId")).persist()

    findf.createOrReplaceTempView("purchases")

    // What are the Top 10 marketing campaigns that bring
    // the biggest revenue (based on billingCost of confirmed purchases)?

    val q1query =
      s"""
         |select campaignId, sum(billingCost) as revenue
         |from purchases
         |where isConfirmed = true
         |group by campaignId
         |order by revenue desc
         |limit 10
         |""".stripMargin

    val q1df = spark.sql(q1query)

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

    q1df.show(false)

    spark.stop()
  }

  def readCsvData(path: String, schema: StructType): DataFrame = {
    spark.read
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(schema)
      .csv(path)
  }

  def filterNeededEvents(clickstreamDf: DataFrame, neededEvents: Seq[String]): DataFrame = {
    clickstreamDf.filter(r => neededEvents.contains(r.getAs[String]("eventType")))
  }

  def normalizeQuotesToNewCol(clickstreamDf: DataFrame): DataFrame = {
    clickstreamDf.withColumn("attrs",
      regexp_replace($"attributes", "[\\“\\”]", "\""))
  }

  def extractAttributes(clickstreamDf: DataFrame): DataFrame = {
    val mapSchema = MapType(StringType, StringType)

    clickstreamDf
      .withColumn("jsonAttrs",
        from_json(regexp_extract($"attrs", "\\{(.*)\\}", 1), mapSchema))
      .select("userId", "eventId", "eventTime", "eventType",
        "jsonAttrs.campaign_id", "jsonAttrs.channel_id", "jsonAttrs.purchase_id")
  }

  def clickstreamTyped(clickstreamDf: DataFrame): Dataset[EventInfo] = {
    clickstreamDf.map { r =>
      EventInfo(
        r.getAs[String]("userId"),
        r.getAs[String]("eventType"),
        r.getAs[String]("campaign_id"),
        r.getAs[String]("channel_id"),
        r.getAs[String]("purchase_id"))
    }
  }

  def aggClickstreamSessions(clickstreamDf: DataFrame): Dataset[(String, List[Map[String, String]])] = {
    val sessAgg = SessionInfoAggregator.toColumn.name("session")

    clickstreamTyped(clickstreamDf).groupByKey(ei => ei.userId).agg(sessAgg)
  }

  def explodeClickstreamSessions(clickstreamDs: Dataset[(String, List[Map[String, String]])]): DataFrame = {
    clickstreamDs.select($"key".as("userId"), explode($"session").as("sessionInfo"))
      .select($"userId", $"sessionInfo.campaignId", $"sessionInfo.channelId", $"sessionInfo.purchase")
  }

  def generateSessionIds(clickstreamDf: DataFrame): DataFrame = {
    clickstreamDf.withColumn("sessionId",
     row_number.over(Window.partitionBy(lit(1)).orderBy("userId")))
  }

  def explodePurchases(clickstreamDf: DataFrame): DataFrame = {
    clickstreamDf.withColumn("purchaseId", split($"purchase", ","))
      .select($"sessionId", $"campaignId", $"channelId", explode_outer($"purchaseId").as("purchaseId"))
      .persist()
  }
}
