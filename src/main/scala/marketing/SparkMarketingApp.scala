package marketing

import marketing.ClickstreamDataTransformations._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._


object SparkMarketingApp extends SparkSessionWrapper {

  private val clickstreamCsvPath = "src/main/resources/marketing/mobile-app-clickstream_sample.csv"
  private val purchasesCsvPath = "src/main/resources/marketing/purchases_sample.csv"
  private val topCampaignsCsvPath = "topTenCampaigns"
  private val topChannelsCsvPath = "topChannels"

  def main(args: Array[String]): Unit = {
    val sessionsDf = getClickstreamSessionsDf(clickstreamCsvPath)
    val purchasesDf = getPurchasesDf(purchasesCsvPath)

    val joinedPurchases = joinPurchasesWithSessions(purchasesDf, sessionsDf).persist()

    val question1df = topTenCampaignsByRevenueSql(joinedPurchases)

    val question2df = topChannelByNumSessionsSql(joinedPurchases)

    saveDfToCsv(question1df, topCampaignsCsvPath)
    saveDfToCsv(question2df, topChannelsCsvPath)

    spark.stop()
  }

  /**
   * Combines all methods needed to construct clickstream DF prepared for a join with purchases DF
   * @param path path to clickstream csv
   * @return DataFrame with schema { sessionId: String, campaignId: String, channelId: String, purchaseId: String }
   */
  def getClickstreamSessionsDf(path: String): DataFrame = {
    val clickstreamSchema = StructType(
      StructField("userId", StringType) ::
        StructField("eventId", StringType) ::
        StructField("eventTime", TimestampType) ::
        StructField("eventType", StringType) ::
        StructField("attributes", StringType) :: Nil
    )

    val clickstreamDf = readCsvData(path, clickstreamSchema)

    val clickstreamFiltered = filterNeededEvents(clickstreamDf, Seq("app_open", "app_close", "purchase"))
    val clickstreamQuotedAttrs = normalizeQuotes(clickstreamFiltered)

    val clickstreamWithAttrs = extractJsonAttributes(clickstreamQuotedAttrs)

    val clickstreamSessions = aggClickstreamSessions(clickstreamWithAttrs)
    val clickstreamWithSessionIds = generateSessionIds(clickstreamSessions)

    explodeSessionPurchases(clickstreamWithSessionIds)
  }


  /**
   * Reads purchases DF from csv
   * @param path path to purchases csv
   * @return DataFrame with schema
   *         { purchaseId: String, purchaseTime: Timestamp, billingCost: Double, isConfirmed: Boolean }
   */
  def getPurchasesDf(path: String): DataFrame = {
    val purchasesSchema = StructType(
      StructField("purchaseId", StringType) ::
        StructField("purchaseTime", TimestampType) ::
        StructField("billingCost", DoubleType) ::
        StructField("isConfirmed", BooleanType) :: Nil
    )

    readCsvData(path, purchasesSchema)
  }


  /**
   * Joins purchases DF with clickstream sessions DF, preserving sessions with no purchases
   * @param purchasesDf DF with schema
   *                    { purchaseId: String, purchaseTime: Timestamp, billingCost: Double, isConfirmed: Boolean }
   * @param sessionsDf DF with schema
   *                   { sessionId: String, campaignId: String, channelId: String, purchaseId: String }
   * @return joined DF with schema
   *         { purchaseId: String, purchaseTime: Timestamp, billingCost: Double, isConfirmed: Boolean,
   *           sessionId: String, campaignId: String, channelId: String }
   */
  def joinPurchasesWithSessions(purchasesDf: DataFrame, sessionsDf: DataFrame): DataFrame = {
    purchasesDf
      .join(sessionsDf, purchasesDf("purchaseId") <=> sessionsDf("purchaseId"), "full")
      .drop(sessionsDf("purchaseId"))
  }


  /**
   * Determines top 10 marketing campaigns with highest revenue
   * @param joinedPurchasesDf purchases DF joined with clickstream sessions DF
   * @return DataFrame with schema { campaignId: String, revenue: Double }
   */
  def topTenCampaignsByRevenueSql(joinedPurchasesDf: DataFrame): DataFrame = {
    val viewName = "purchases"
    joinedPurchasesDf.createOrReplaceTempView(viewName)

    spark.sql(topTenCampaignsByRevenueSqlQuery(viewName))
  }

  /**
   * SQL query for topTenCampaignsByRevenueSql
   */
  def topTenCampaignsByRevenueSqlQuery(viewName: String): String =
    s"""
       |select campaignId, sum(billingCost) as revenue
       |from $viewName
       |where isConfirmed = true
       |group by campaignId
       |order by revenue desc
       |limit 10
       |""".stripMargin


  /**
   * Determines channel with highest amount of unique sessions in each campaign
   * @param joinedPurchasesDf purchases DF joined with clickstream sessions DF
   * @return DataFrame with schema { campaignId: String, topChannel: String }
   */
  def topChannelByNumSessionsSql(joinedPurchasesDf: DataFrame): DataFrame = {
    val viewName = "purchases"
    joinedPurchasesDf.createOrReplaceTempView(viewName)

    spark.sql(topChannelByNumSessionsSqlQuery(viewName))
  }

  /**
   * SQL query for topChannelByNumSessionsSql
   */
  def topChannelByNumSessionsSqlQuery(viewName: String): String =
    s"""
       |select campaignId, first(channelId) as topChannel from
       |     (select campaignId, channelId
       |      from $viewName
       |      group by campaignId, channelId
       |      order by count(distinct sessionId) desc)
       |group by campaignId
       |""".stripMargin


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
