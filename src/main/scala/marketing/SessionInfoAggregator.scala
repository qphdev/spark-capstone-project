package marketing

import marketing.MarketingDataPreprocessing.SessionInfo
import marketing.SessionInfoAggregator.SessionInfoAgg

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable.ListBuffer


class SessionInfoAggregator extends Aggregator[SessionInfo, SessionInfoAgg, SessionInfoAgg]{
  override def zero: SessionInfoAgg = SessionInfoAgg("", "", ListBuffer.empty[String])

  override def reduce(buff: SessionInfoAgg, in: SessionInfo): SessionInfoAgg = {
    in.eventType match {
      case "app_open" if in.campaignId.isDefined && in.channelId.isDefined =>
        buff.campaignId = in.campaignId.get
        buff.channelId = in.channelId.get
        buff

      case "purchase" if in.purchaseId.isDefined =>
        buff.purchaseId += in.purchaseId.get
        buff

      case _ =>
        buff
    }
  }

  override def merge(b1: SessionInfoAgg, b2: SessionInfoAgg): SessionInfoAgg = {
    SessionInfoAgg(b1.campaignId + b2.campaignId, b1.channelId + b2.channelId, b1.purchaseId ++ b2.purchaseId)
  }

  override def finish(red: SessionInfoAgg): SessionInfoAgg = red

  override def bufferEncoder: Encoder[SessionInfoAgg] = Encoders.product[SessionInfoAgg]

  override def outputEncoder: Encoder[SessionInfoAgg] = Encoders.product[SessionInfoAgg]
}


object SessionInfoAggregator {
  case class SessionInfoAgg(var campaignId: String, var channelId: String, purchaseId: ListBuffer[String])
}