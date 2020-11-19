package marketing

import marketing.MarketingDataSQLAnalysis.{topChannelByNumSessionsSql, topCampaignsByRevenueSql}
import marketing.MarketingDataPreprocessing._
import marketing.MarketingDataUtils.saveDfToCsv


object SparkMarketingApp extends SparkSessionWrapper {

  private val topCampaignsCsvPath = "topTenCampaigns"
  private val topChannelsCsvPath = "topChannels"

  def main(args: Array[String]): Unit = {
    val sessionsDf = getClickstreamSessionsDf(args(0))
    val purchasesDf = getPurchasesDf(args(1))

    val joinedPurchases = joinPurchasesWithSessions(purchasesDf, sessionsDf).cache()

    val question1df = topCampaignsByRevenueSql(joinedPurchases)
    val question2df = topChannelByNumSessionsSql(joinedPurchases)

    saveDfToCsv(question1df, topCampaignsCsvPath)
    saveDfToCsv(question2df, topChannelsCsvPath)

    spark.stop()
  }

}
