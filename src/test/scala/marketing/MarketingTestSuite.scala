package marketing

import marketing.ClickstreamDataTransformations.{aggClickstreamSessions, explodeSessionPurchases,
  extractJsonAttributes, generateSessionIds}
import marketing.SparkMarketingApp.{joinPurchasesWithSessions, topChannelByNumSessionsSql, topTenCampaignsByRevenueSql}
import java.sql.Timestamp

import org.apache.spark.sql.Row
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite


class MarketingTestSuite extends FunSuite with DataFrameSuiteBase {

  private val finalDfSchema = StructType(
    StructField("purchaseId", StringType) ::
      StructField("purchaseTime", TimestampType) ::
      StructField("billingCost", DoubleType) ::
      StructField("isConfirmed", BooleanType) ::
      StructField("sessionId", StringType) ::
      StructField("campaignId", StringType) ::
      StructField("channelId", StringType) :: Nil
  )


  test("extractJsonAttributes returns DF with cols campaign_id, channel_id, purchase_id") {
    import spark.implicits._

    val df = Seq(
      ("u1", "1", Timestamp.valueOf("2000-01-01 10:10:10"), "o", "{{\"campaign_id\": \"c1\", \"channel_id\": \"ch1\"}}"),
      ("u2", "2", Timestamp.valueOf("2001-01-01 10:10:10"), "p", "{{\"purchase_id\": \"p1\"}}"),
      ("u2", "3", Timestamp.valueOf("2001-01-01 10:11:10"), "c", null)
    ).toDF("userId", "eventId", "eventTime", "eventType", "attrs")

    val actualRes = extractJsonAttributes(df)
    val expectedRes = Seq(
      ("u1", "o", "c1", "ch1", null),
      ("u2", "p", null, null, "p1"),
      ("u2", "c", null, null, null)
    ).toDF("userId", "eventType", "campaign_id", "channel_id", "purchase_id")

    assertDataFrameEquals(actualRes, expectedRes)
  }


  test("aggClickstreamSessions collects each session info into one row") {
    import spark.implicits._

    val df = Seq(
      ("u1", "app_open", "c1", "ch1", null),
      ("u1", "purchase", null, null, "p1"),
      ("u1", "app_close", null, null, null),
      ("u1", "app_open", "c2", "ch2", null),
      ("u1", "purchase", null, null, "p2"),
      ("u1", "purchase", null, null, "p3"),
      ("u1", "app_close", null, null, null),
      ("u2", "app_open", "c2", "ch3", null),
      ("u2", "app_close", null, null, null)
    ).toDF("userId", "eventType", "campaign_id", "channel_id", "purchase_id")

    val actualRes = aggClickstreamSessions(df)

    val expectedRes = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("u1", "c1", "ch1", "p1"),
          Row("u1", "c2", "ch2", "p2,p3"),
          Row("u2", "c2", "ch3", null))),
      StructType(
        StructField("userId", StringType) ::
        StructField("campaignId", StringType) ::
        StructField("channelId", StringType) ::
        StructField("purchase", StringType) :: Nil)
    )

    assertDataFrameEquals(actualRes, expectedRes)
  }


  test("generateSessionIds generates unique id for each session") {
    import spark.implicits._

    val df = Seq(
      ("u1", "c1", "ch1", "p1"),
      ("u1", "c2", "ch2", "p2,p3"),
      ("u2", "c2", "ch3", null)
    ).toDF("userId", "campaignId", "channelId", "purchase")

    val actualRes = generateSessionIds(df).select("sessionId")

    val expectedRes = Seq("s1", "s2", "s3").toDF("sessionId")

    assertDataFrameDataEquals(actualRes, expectedRes)
  }


  test("explodeSessionPurchases returns one row per purchase in session") {
    import spark.implicits._

    val df = Seq(
      ("u1", "c1", "ch1", "p1", "s1"),
      ("u1", "c2", "ch2", "p2,p3,p4", "s2"),
      ("u2", "c2", "ch3", null, "s3")
    ).toDF("userId", "campaignId", "channelId", "purchase", "sessionId")

    val res = explodeSessionPurchases(df)
    val sessionIds = res.select("sessionId")

    val expectedIds = Seq("s1", "s2", "s2", "s2", "s3").toDF("sessionId")

    assertDataFrameDataEquals(sessionIds, expectedIds)
  }


  test("joinPurchasesWithSessions result contains all sessions and purchases") {
    import spark.implicits._

    val purDf = Seq(
      ("p1", Timestamp.valueOf("2000-01-01 10:10:10"), 100.1, true),
      ("p2", Timestamp.valueOf("2000-01-02 10:10:10"), 50.2, true),
      ("p3", Timestamp.valueOf("2000-01-03 10:10:10"), 200.0, false)
    ).toDF("purchaseId", "purchaseTime", "billingCost", "isConfirmed")

    val sessDf = Seq(
      ("s1", "c1", "ch1", "p1"),
      ("s2", "c2", "ch2", "p2"),
      ("s2", "c2", "ch1", "p3"),
      ("s3", "c1", "ch2", null)
    ).toDF("sessionId", "campaignId", "channelId", "purchaseId")

    val actualRes = joinPurchasesWithSessions(purDf, sessDf)

    val expectedRes = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(null, null, null, null, "s3", "c1", "ch2"),
          Row("p1", Timestamp.valueOf("2000-01-01 10:10:10"), 100.1, true, "s1", "c1", "ch1"),
          Row("p2", Timestamp.valueOf("2000-01-02 10:10:10"), 50.2, true, "s2", "c2", "ch2"),
          Row("p3", Timestamp.valueOf("2000-01-03 10:10:10"), 200.0, false, "s2", "c2", "ch1"))),
      finalDfSchema)

    assertDataFrameNoOrderEquals(actualRes, expectedRes)
  }


  test("topTenCampaignsByRevenueSql") {
    import spark.implicits._

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(null, null, null, null, "s1", "c1", "ch2"),
          Row("p1", Timestamp.valueOf("2000-01-01 10:10:10"), 100.1, true, "s2", "c1", "ch1"),
          Row("p2", Timestamp.valueOf("2000-02-01 10:10:10"), 50.2, false, "s2", "c2", "ch2"),
          Row("p3", Timestamp.valueOf("2000-03-01 10:10:10"), 150.5, true, "s3", "c2", "ch3"),
          Row("p4", Timestamp.valueOf("2000-04-01 10:10:10"), 80.9, true, "s4", "c1", "ch1"))),
        finalDfSchema
    )

    val actualRes = topTenCampaignsByRevenueSql(df)

    val expectedRes = Seq(
      ("c1", 181.0),
      ("c2", 150.5)
    ).toDF("campaignId", "revenue")

    assertDataFrameDataEquals(actualRes, expectedRes)
  }


  test("topChannelByNumSessionsSql") {
    import spark.implicits._

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(null, null, null, null, "s1", "c1", "ch2"),
          Row("p1", Timestamp.valueOf("2000-01-01 10:10:10"), 100.1, true, "s2", "c1", "ch1"),
          Row("p2", Timestamp.valueOf("2000-02-01 10:10:10"), 50.2, false, "s2", "c2", "ch2"),
          Row("p3", Timestamp.valueOf("2000-03-01 10:10:10"), 150.5, true, "s3", "c2", "ch3"),
          Row("p4", Timestamp.valueOf("2000-04-01 10:10:10"), 80.9, true, "s4", "c1", "ch1"))),
      finalDfSchema
    )

    val actualRes = topChannelByNumSessionsSql(df)

    val expectedRes = Seq(
      ("c1", "ch1"),
      ("c2", "ch2")
    ).toDF("campaignId","topChannel")

    assertDataFrameDataEquals(actualRes, expectedRes)
  }
}
