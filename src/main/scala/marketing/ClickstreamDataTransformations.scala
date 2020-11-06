package marketing

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{explode, explode_outer, from_json, lit,
  regexp_extract, regexp_replace, row_number, split, concat}
import org.apache.spark.sql.types.{MapType, StringType}


object ClickstreamDataTransformations extends SparkSessionWrapper {

  import spark.implicits._

  /**
   * Filters out rows with unneeded eventTypes from clickstreamDF
   */
  def filterNeededEvents(clickstreamDf: DataFrame, neededEvents: Seq[String]): DataFrame = {
    clickstreamDf.filter(r => neededEvents.contains(r.getAs[String]("eventType")))
  }


  /**
   * Replaces quotes '“', '”' with default '"' in attributes column
   */
  def normalizeQuotes(clickstreamDf: DataFrame): DataFrame = {
    clickstreamDf
      .withColumn("attrs", regexp_replace($"attributes", "[\\“\\”]", "\""))
      .drop($"attributes")
  }


  /**
   * Extracts campaign_id, channel_id, purchase_id from json in "attrs" column
   * @param clickstreamDf clickstream DF with normalized quotes
   * @return clickstream DF with schema
   *         { userId: String, eventType: String, campaign_id: String, channel_id: String, purchase_id: String }
   */
  def extractJsonAttributes(clickstreamDf: DataFrame): DataFrame = {
    val mapSchema = MapType(StringType, StringType)

    clickstreamDf
      .withColumn("jsonAttrs",
        from_json(regexp_extract($"attrs", "\\{(.*)\\}", 1), mapSchema))
      .select("userId", "eventType",
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


  /**
   * Aggregates events related to the same session
   * @param clickstreamDf clickstream DF with schema
   *                      { userId: String, eventType: String, campaign_id: String,
   *                        channel_id: String, purchase_id: String }
   * @return DF of clickstream sessions
   */
  def aggClickstreamSessions(clickstreamDf: DataFrame): DataFrame = {
    val sessionAgg = SessionInfoAggregator.toColumn.name("session")

    val typedGrouped = clickstreamTyped(clickstreamDf).repartition($"userId")
      .groupByKey(ei => ei.userId)

    explodeClickstreamSessions(typedGrouped.agg(sessionAgg))
  }


  /**
   * Extracts sessions from aggregation result ( userId, all sessions of this user )
   */
  def explodeClickstreamSessions(clickstreamDs: Dataset[(String, List[Map[String, String]])]): DataFrame = {
    clickstreamDs.select($"key".as("userId"), explode($"session").as("sessionInfo"))
      .select($"userId", $"sessionInfo.campaignId", $"sessionInfo.channelId", $"sessionInfo.purchase")
  }


  /**
   * Generates unique id for each session in DF
   * @param clickstreamDf DF of clickstream sessions
   */
  def generateSessionIds(clickstreamDf: DataFrame): DataFrame = {
    clickstreamDf
      .withColumn("sessionId",
        concat(lit("s"), row_number.over(Window.partitionBy(lit(1)).orderBy("userId"))))
  }


  /**
   * Explodes rows with multiple purchases per session
   */
  def explodeSessionPurchases(clickstreamDf: DataFrame): DataFrame = {
    clickstreamDf.withColumn("purchaseId", split($"purchase", ","))
      .select($"sessionId", $"campaignId", $"channelId", explode_outer($"purchaseId").as("purchaseId"))
  }
}


case class EventInfo(userId: String, eventType: String,
                     campaignId: String, channelId: String, purchaseId: String)