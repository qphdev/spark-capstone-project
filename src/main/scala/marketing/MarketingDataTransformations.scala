package marketing

import marketing.MarketingDataUtils.{normalizeQuotes, readCsvData}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{concat, from_json, last, lit, regexp_extract, udaf, when}
import org.apache.spark.sql.types.{BooleanType, DoubleType, MapType, StringType, StructField, StructType, TimestampType}


object MarketingDataTransformations extends SparkSessionWrapper {

  private val clickstreamSchema = StructType(
    StructField("userId", StringType) ::
      StructField("eventId", StringType) ::
      StructField("eventTime", TimestampType) ::
      StructField("eventType", StringType) ::
      StructField("attributes", StringType) :: Nil
  )

  private val purchasesSchema = StructType(
    StructField("purchaseId", StringType) ::
      StructField("purchaseTime", TimestampType) ::
      StructField("billingCost", DoubleType) ::
      StructField("isConfirmed", BooleanType) :: Nil
  )

  import spark.implicits._

  /**
   * Combines all methods needed to construct clickstream DF prepared for a join with purchases DF
   * @param path path to clickstream csv
   * @return DataFrame with schema { sessionId: String, campaignId: String, channelId: String, purchaseId: String }
   */
  def getClickstreamSessionsDf(path: String): DataFrame = {
    val clickstreamDf = readCsvData(path, clickstreamSchema)

    val clickstreamFiltered = filterNeededEvents(clickstreamDf, Seq("app_open", "app_close", "purchase"))
    val clickstreamQuotedAttrs = normalizeQuotes(clickstreamFiltered)

    val clickstreamWithAttrs = extractJsonAttributes(clickstreamQuotedAttrs)
    val clickstreamSessionIds = generateSessionIds(clickstreamWithAttrs)

    aggregateClickstreamSessionsSQL(clickstreamSessionIds)
  }


  /**
   * Reads purchases DF from csv
   * @param path path to purchases csv
   * @return DataFrame with schema
   *         { purchaseId: String, purchaseTime: Timestamp, billingCost: Double, isConfirmed: Boolean }
   */
  def getPurchasesDf(path: String): DataFrame = {
    readCsvData(path, purchasesSchema)
  }


  /**
   * Filters out rows with unneeded eventTypes from clickstreamDF
   */
  private def filterNeededEvents(clickstreamDf: DataFrame, neededEvents: Seq[String]): DataFrame = {
    clickstreamDf.filter(r => neededEvents.contains(r.getAs[String]("eventType")))
  }


  /**
   * Extracts campaignId, channelId, purchaseId from json in "attrs" column
   * @param clickstreamDf clickstream DF with normalized quotes
   * @return clickstream DF with schema
   *         { userId: String, eventId: String, eventTime: TimeStamp, eventType: String,
   *           campaign_id: String, channel_id: String, purchase_id: String }
   */
  def extractJsonAttributes(clickstreamDf: DataFrame): DataFrame = {
    val mapSchema = MapType(StringType, StringType)

    clickstreamDf
      .withColumn("jsonAttrs",
        from_json(regexp_extract($"attrs", "\\{(.*)\\}", 1), mapSchema))
      .select($"userId", $"eventId", $"eventTime", $"eventType",
        $"jsonAttrs.campaign_id".alias("campaignId"),
        $"jsonAttrs.channel_id".alias("channelId"),
        $"jsonAttrs.purchase_id".alias("purchaseId"))
  }


  /**
   * Generates unique sessionId for each session
   * @param clickstreamDf clickstream DF with userId, eventId, eventTime, eventType columns present
   * @return clickstream DF with sessionId column
   */
  def generateSessionIds(clickstreamDf: DataFrame): DataFrame = {
    val byUserIdOrdByEventTime = Window.partitionBy($"userId").orderBy($"eventTime")

    clickstreamDf
      .withColumn("open", when($"eventType" === "app_open", $"eventId"))
      .withColumn("lastOpen",
        last($"open", ignoreNulls = true).over(byUserIdOrdByEventTime.rowsBetween(Window.unboundedPreceding, 0)))
      .withColumn("sessionId", concat(lit("s"), $"lastOpen"))
      .drop("open", "lastOpen")
  }


  /**
   * Collects campaignId, channelId, purchaseId related to the same session into one row
   * Multiple purchases per session are exploded
   * @param clickstreamDf clickstream DF with sessionId column present
   * @return DF with schema { sessionId: String, campaignId: String, channelId: String, purchaseId: String }
   */
  def aggregateClickstreamSessionsSQL(clickstreamDf: DataFrame): DataFrame = {
    val aggUdaf = udaf(new SessionInfoAggregator())
    spark.udf.register("aggSessions", aggUdaf)

    val clickstreamTyped = clickstreamSessionInfoDs(clickstreamDf)
    clickstreamTyped.createOrReplaceTempView("sessions")

    val aggQuery =
      s"""
         |select sessionId, session.campaignId, session.channelId,
         |       explode_outer(session.purchaseId) as purchaseId
         |from (select sessionId, aggSessions(*) as session
         |      from sessions
         |      group by sessionId)
         |""".stripMargin

    spark.sql(aggQuery)
  }


  /** Selects columns needed for session aggregation and transforms
   *  clickstream DF into a Dataset[SessionInfo]
   *  */
  def clickstreamSessionInfoDs(clickstreamDf: DataFrame): Dataset[SessionInfo] = {
    clickstreamDf
      .select($"sessionId", $"eventType", $"campaignId", $"channelId", $"purchaseId")
      .as[SessionInfo]
  }


  case class SessionInfo(sessionId: String, eventType: String, campaignId: Option[String],
                         channelId: Option[String], purchaseId: Option[String])
}