package marketing

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class MarketingTestSuite extends AnyFunSuite with BeforeAndAfterAll with SparkSessionWrapper {

  test("extractJsonAttributes returns DF with campaign_id, channel_id, purchase_id") {

  }

  override def afterAll(): Unit = {
    spark.stop()
    super.afterAll()
  }
}
