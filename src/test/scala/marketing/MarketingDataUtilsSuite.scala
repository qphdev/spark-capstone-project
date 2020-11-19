package marketing

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import marketing.MarketingDataUtils.normalizeQuotes
import org.scalatest.FunSuite

class MarketingDataUtilsSuite extends FunSuite with DataFrameSuiteBase {

  test("normalizeQuotes replaces tilted quotes with regular ones") {
    import spark.implicits._

    val df = Seq(
      "\"ok“notok",
      "”notok\"ok",
      "\"ok\"ok",
      ""
    ).toDF("attributes")

    val actualRes = normalizeQuotes(df)

    val expectedRes = Seq(
      "\"ok\"notok",
      "\"notok\"ok",
      "\"ok\"ok",
      ""
    ).toDF("attrs")

    assertDataFrameEquals(actualRes, expectedRes)
  }
}
