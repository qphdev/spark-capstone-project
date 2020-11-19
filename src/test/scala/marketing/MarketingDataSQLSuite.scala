package marketing

import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import marketing.MarketingDataSQLAnalysis.{topChannelByNumSessionsSql, topCampaignsByRevenueSql}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType, TimestampType}
import org.scalatest.FunSuite


class MarketingDataSQLSuite extends FunSuite with DataFrameSuiteBase  {

  private val joinedDfSchema = StructType(
    StructField("purchaseId", StringType) ::
      StructField("purchaseTime", TimestampType) ::
      StructField("billingCost", DoubleType) ::
      StructField("isConfirmed", BooleanType) ::
      StructField("sessionId", StringType) ::
      StructField("campaignId", StringType) ::
      StructField("channelId", StringType) :: Nil
  )

  test("topCampaignsByRevenueSql") {
    import spark.implicits._

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(
        Seq(
          Row(null, null, null, null, "s1", "c1", "ch2"),
          Row("p1", Timestamp.valueOf("2000-01-01 10:10:10"), 100.1, true, "s2", "c1", "ch1"),
          Row("p2", Timestamp.valueOf("2000-02-01 10:10:10"), 50.2, false, "s2", "c2", "ch2"),
          Row("p3", Timestamp.valueOf("2000-03-01 10:10:10"), 150.5, true, "s3", "c2", "ch3"),
          Row("p4", Timestamp.valueOf("2000-04-01 10:10:10"), 80.9, true, "s4", "c1", "ch1"))),
      joinedDfSchema
    )

    val actualRes = topCampaignsByRevenueSql(df)

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
      joinedDfSchema
    )

    val actualRes = topChannelByNumSessionsSql(df)

    val expectedRes = Seq(
      ("c1", "ch1"),
      ("c2", "ch2")
    ).toDF("campaignId","topChannel")

    assertDataFrameDataEquals(actualRes, expectedRes)
  }
}
