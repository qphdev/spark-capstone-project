package marketing

import marketing.MarketingDataTransformations._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._


object SparkMarketingApp extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {

    val clickstreamSchema = StructType(
      StructField("userId", StringType) ::
        StructField("eventId", StringType) ::
        StructField("eventTime", TimestampType) ::
        StructField("eventType", StringType) ::
        StructField("attributes", StringType) :: Nil
    )

    val clickstreamDf = readCsvData(
      "file:///Users/mtoporova/Documents/csvData/mobile-app-clickstream_sample.csv",
      clickstreamSchema)

    val clickstreamFiltered = filterNeededEvents(clickstreamDf, Seq("app_open", "app_close", "purchase"))
    val clickstreamQuotedAttrs = normalizeQuotesToNewCol(clickstreamFiltered)

    val clickstreamWithAttrs = extractJsonAttributes(clickstreamQuotedAttrs)

    val clickstreamSessionsPerUser = aggClickstreamSessions(clickstreamWithAttrs)
    val clickstreamSessions = explodeClickstreamSessions(clickstreamSessionsPerUser)
    val clickstreamWithSessionIds = generateSessionIds(clickstreamSessions)
    val sessionsDf = explodeSessionPurchases(clickstreamWithSessionIds)

    val purchasesSchema = StructType(
      StructField("purchaseId", StringType) ::
        StructField("purchaseTime", TimestampType) ::
        StructField("billingCost", DoubleType) ::
        StructField("isConfirmed", BooleanType) :: Nil
    )

    val purchasesDf = readCsvData(
      "file:///Users/mtoporova/Documents/csvData/purchases_sample - purchases_sample.csv",
      purchasesSchema)

    val joinedPurchases = joinPurchasesWithSessions(purchasesDf, sessionsDf)

    val q1df = topTenCampaignsByRevenueSql(joinedPurchases)

    val q2df = topChannelByNumSessionsSql(joinedPurchases)

    q2df.show(false)
    q1df.show(false)

    clickstreamQuotedAttrs.show(false)
    spark.stop()
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
}
