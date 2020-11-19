package marketing

import org.apache.spark.sql.DataFrame


object MarketingDataSQL extends SparkSessionWrapper {
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
    purchasesDf.createOrReplaceTempView("purchase")
    sessionsDf.createOrReplaceTempView("session")

    val joinQuery =
      s"""
         |select purchase.purchaseId, purchaseTime, billingCost, isConfirmed, sessionId, campaignId, channelId
         |from purchase full outer join session
         |              on purchase.purchaseId <=> session.purchaseId
         |""".stripMargin

    spark.sql(joinQuery)
  }


  /**
   * Determines top N marketing campaigns with highest revenue
   * @param joinedPurchasesDf purchases DF joined with clickstream sessions DF
   * @param limit N
   * @return DataFrame with schema { campaignId: String, revenue: Double }
   */
  def topCampaignsByRevenueSql(joinedPurchasesDf: DataFrame, limit: Int = 10): DataFrame = {
    joinedPurchasesDf.createOrReplaceTempView("purchases")

    val topTenCampsQuery =
      s"""
         |select campaignId, sum(billingCost) as revenue
         |from purchases
         |where isConfirmed = true
         |group by campaignId
         |order by revenue desc
         |limit $limit
         |""".stripMargin

    spark.sql(topTenCampsQuery)
  }


  /**
   * Determines channel with highest amount of unique sessions in each campaign
   * @param joinedPurchasesDf purchases DF joined with clickstream sessions DF
   * @return DataFrame with schema { campaignId: String, topChannel: String }
   */
  def topChannelByNumSessionsSql(joinedPurchasesDf: DataFrame): DataFrame = {
    joinedPurchasesDf.createOrReplaceTempView("purchases")

    val topChannelQuery =
      s"""
       |select campaignId, first(channelId) as topChannel from
       |     (select campaignId, channelId
       |      from purchases
       |      group by campaignId, channelId
       |      order by count(distinct sessionId) desc)
       |group by campaignId
       |""".stripMargin

    spark.sql(topChannelQuery)
  }
}
