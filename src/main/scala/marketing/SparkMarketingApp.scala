package marketing

import marketing.MarketingData.readCsvData
import marketing.MarketingData._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._


object SparkMarketingApp extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    val sessionsDf = getClickstreamSessionsDf(args(0))
    val purchasesDf = getPurchasesDf(args(1))

    val joinedPurchases = joinPurchasesWithSessions(purchasesDf, sessionsDf)

    val q1df = topTenCampaignsByRevenueSql(joinedPurchases)

    val q2df = topChannelByNumSessionsSql(joinedPurchases)

    saveDfToCsv(q1df, args(2))
    saveDfToCsv(q2df, args(3))

    spark.stop()
  }


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
    val clickstreamQuotedAttrs = normalizeQuotesToNewCol(clickstreamFiltered)

    val clickstreamWithAttrs = extractJsonAttributes(clickstreamQuotedAttrs)

    val clickstreamSessions = aggClickstreamSessions(clickstreamWithAttrs)
    val clickstreamWithSessionIds = generateSessionIds(clickstreamSessions)

    explodeSessionPurchases(clickstreamWithSessionIds)
  }


  def getPurchasesDf(path: String): DataFrame = {
    val purchasesSchema = StructType(
      StructField("purchaseId", StringType) ::
        StructField("purchaseTime", TimestampType) ::
        StructField("billingCost", DoubleType) ::
        StructField("isConfirmed", BooleanType) :: Nil
    )

    readCsvData(path, purchasesSchema)
  }


  def joinPurchasesWithSessions(purchasesDf: DataFrame, sessionsDf: DataFrame): DataFrame = {
    purchasesDf
      .join(sessionsDf, purchasesDf("purchaseId") <=> sessionsDf("purchaseId"), "full")
      .drop(sessionsDf("purchaseId"))
  }


  def topTenCampaignsByRevenueSql(joinedPurchasesDf: DataFrame): DataFrame = {
    val viewName = "purchases"
    joinedPurchasesDf.createOrReplaceTempView(viewName)

    spark.sql(topTenCampaignsByRevenueSqlQuery(viewName))
  }

  def topTenCampaignsByRevenueSqlQuery(viewName: String): String =
    s"""
       |select campaignId, sum(billingCost) as revenue
       |from $viewName
       |where isConfirmed = true
       |group by campaignId
       |order by revenue desc
       |limit 10
       |""".stripMargin


  def topChannelByNumSessionsSql(joinedPurchasesDf: DataFrame): DataFrame = {
    val viewName = "purchases"
    joinedPurchasesDf.createOrReplaceTempView(viewName)

    spark.sql(topChannelByNumSessionsSqlQuery(viewName))
  }

  def topChannelByNumSessionsSqlQuery(viewName: String): String =
    s"""
       |select campaignId, first(channelId) as topChannel from
       |     (select campaignId, channelId
       |      from $viewName
       |      group by campaignId, channelId
       |      order by count(distinct sessionId) desc)
       |group by campaignId
       |""".stripMargin


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
