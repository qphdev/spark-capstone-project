package marketing

import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import marketing.MarketingDataTransformations.{aggregateClickstreamSessionsSQL, extractJsonAttributes, generateSessionIds}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.FunSuite

class MarketingDataTransformationsSuite extends FunSuite with DataFrameSuiteBase  {

  test("extractJsonAttributes returns DF with cols campaign_id, channel_id, purchase_id") {
    import spark.implicits._

    val df = Seq(
      ("u1", "1", Timestamp.valueOf("2000-01-01 10:10:10"), "o", "{{\"campaign_id\": \"c1\", \"channel_id\": \"ch1\"}}"),
      ("u2", "2", Timestamp.valueOf("2001-01-01 10:10:10"), "p", "{{\"purchase_id\": \"p1\"}}"),
      ("u2", "3", Timestamp.valueOf("2001-01-01 10:11:10"), "c", null)
    ).toDF("userId", "eventId", "eventTime", "eventType", "attrs")

    val actualRes = extractJsonAttributes(df)
    val expectedRes = Seq(
      ("u1", "1", Timestamp.valueOf("2000-01-01 10:10:10"), "o", "c1", "ch1", null),
      ("u2", "2", Timestamp.valueOf("2001-01-01 10:10:10"), "p", null, null, "p1"),
      ("u2", "3", Timestamp.valueOf("2001-01-01 10:11:10"), "c", null, null, null)
    ).toDF("userId", "eventId", "eventTime", "eventType", "campaignId", "channelId", "purchaseId")

    assertDataFrameDataEquals(actualRes, expectedRes)
  }


  test("generateSessionIds generates unique id for each session") {
    import spark.implicits._

    val df = Seq(
      ("u1", "e1", Timestamp.valueOf("2000-01-01 11:11:11"), "app_open"),
      ("u1", "e2", Timestamp.valueOf("2000-01-01 11:11:12"), "purchase"),
      ("u2", "e6", Timestamp.valueOf("2000-01-01 11:11:11"), "app_close"),
      ("u1", "e3", Timestamp.valueOf("2000-01-01 11:11:13"), "purchase"),
      ("u1", "e4", Timestamp.valueOf("2000-01-01 11:11:14"), "app_close"),
      ("u2", "e5", Timestamp.valueOf("2000-01-01 11:11:10"), "app_open")
    ).toDF("userId", "eventId", "eventTime", "eventType")

    val actualRes = generateSessionIds(df).select($"sessionId").distinct()

    val expectedRes = Seq("se1", "se5").toDF("sessionId")

    assertDataFrameDataEquals(actualRes, expectedRes)
  }


  test("aggregateClickstreamSessionsSQL collects each session info into one row") {
    import spark.implicits._

    val df = Seq(
      ("s1", "app_open", "c1", "ch1", null),
      ("s1", "purchase", null, null, "p1"),
      ("s2", "app_open", "c2", "ch2", null),
      ("s2", "purchase", null, null, "p2"),
      ("s3", "app_open", "c2", "ch3", null),
      ("s3", "app_close", null, null, null),
      ("s2", "purchase", null, null, "p3"),
      ("s2", "purchase", null, null, "p4"),
      ("s2", "app_close", null, null, null),
      ("s1", "app_close", null, null, null)
    ).toDF("sessionId", "eventType", "campaignId", "channelId", "purchaseId")

    val actualRes = aggregateClickstreamSessionsSQL(df)

    val expectedRes = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row("s1", "c1", "ch1", "p1"),
          Row("s2", "c2", "ch2", "p2"),
          Row("s2", "c2", "ch2", "p3"),
          Row("s2", "c2", "ch2", "p4"),
          Row("s3", "c2", "ch3", null))),
      StructType(
        StructField("sessionId", StringType) ::
          StructField("campaignId", StringType) ::
          StructField("channelId", StringType) ::
          StructField("purchaseId", StringType) :: Nil)
    )

    assertDataFrameDataEquals(actualRes, expectedRes)
  }
}
