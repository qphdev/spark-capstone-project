package marketing

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{explode, explode_outer, from_json, lit,
  regexp_extract, regexp_replace, row_number, split, concat}
import org.apache.spark.sql.types.{MapType, StringType, StructType}


case class EventInfo(userId: String, eventType: String,
                     campaignId: String, channelId: String, purchaseId: String)


object MarketingData extends SparkSessionWrapper {

  import spark.implicits._

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

  def extractJsonAttributes(clickstreamDf: DataFrame): DataFrame = {
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

  def aggClickstreamSessions(clickstreamDf: DataFrame): DataFrame = {
    val sessionAgg = SessionInfoAggregator.toColumn.name("session")

    val typedGrouped = clickstreamTyped(clickstreamDf).groupByKey(ei => ei.userId)

    explodeClickstreamSessions(typedGrouped.agg(sessionAgg))
  }

  def explodeClickstreamSessions(clickstreamDs: Dataset[(String, List[Map[String, String]])]): DataFrame = {
    clickstreamDs.select($"key".as("userId"), explode($"session").as("sessionInfo"))
      .select($"userId", $"sessionInfo.campaignId", $"sessionInfo.channelId", $"sessionInfo.purchase")
  }

  def generateSessionIds(clickstreamDf: DataFrame): DataFrame = {
    clickstreamDf
      .withColumn("sessionId",
        concat(lit("s"), row_number.over(Window.partitionBy(lit(1)).orderBy("userId"))))
  }

  def explodeSessionPurchases(clickstreamDf: DataFrame): DataFrame = {
    clickstreamDf.withColumn("purchaseId", split($"purchase", ","))
      .select($"sessionId", $"campaignId", $"channelId", explode_outer($"purchaseId").as("purchaseId"))
  }
}
