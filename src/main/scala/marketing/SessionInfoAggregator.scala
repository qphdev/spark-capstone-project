package marketing

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class SessionInfoAggBuffer(currIdx: Int, maps: ListBuffer[mutable.Map[String, String]])


object SessionInfoAggregator extends Aggregator[EventInfo, SessionInfoAggBuffer, List[Map[String, String]]] {
  def zero: SessionInfoAggBuffer = SessionInfoAggBuffer(0, ListBuffer.empty[mutable.Map[String, String]])

  def reduce(buff: SessionInfoAggBuffer, in: EventInfo): SessionInfoAggBuffer = {
    in.eventType match {
      case "app_open" =>
        buff.maps += mutable.Map.empty[String, String]

        buff.maps(buff.currIdx)("campaignId") = in.campaignId
        buff.maps(buff.currIdx)("channelId") = in.channelId
        println(s"op:$buff")
        buff

      case "purchase" =>
        buff.maps(buff.currIdx).get("purchase") match {
          case Some(_) =>
            buff.maps(buff.currIdx)("purchase") += s",${in.purchaseId}"
            println(s"pur:$buff")
            buff

          case None =>
            buff.maps(buff.currIdx)("purchase") = in.purchaseId
            println(s"pur:$buff")
            buff
        }

      case "app_close" =>
        println(s"clo:$buff")
        buff.copy(currIdx = buff.currIdx + 1)
    }
  }

  def merge(b1: SessionInfoAggBuffer, b2: SessionInfoAggBuffer): SessionInfoAggBuffer =
    SessionInfoAggBuffer(0, b1.maps ++ b2.maps)


  def finish(red: SessionInfoAggBuffer): List[Map[String, String]] = {
    red.maps.map(_.toMap).toList
  }

  def bufferEncoder: Encoder[SessionInfoAggBuffer] = Encoders.product[SessionInfoAggBuffer]

  def outputEncoder: Encoder[List[Map[String, String]]] =
    implicitly(ExpressionEncoder[List[Map[String, String]]])
}
