package com.infixtrading.flashbot.sources

import java.net.URI

import akka.NotUsed
import akka.actor.{ActorContext, ActorRef, PoisonPill}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import com.infixtrading.flashbot.core.DataSource.IngestGroup
import com.infixtrading.flashbot.core.DataType.{OrderBookType, TradesType}
import com.infixtrading.flashbot.core.Instrument.CurrencyPair
import com.infixtrading.flashbot.core._
import com.infixtrading.flashbot.models.core.Order.{OrderType, Side}
import com.infixtrading.flashbot.models.core.OrderBook
import com.infixtrading.flashbot.util.time.TimeFmt
import com.infixtrading.flashbot.util
import io.circe.generic.JsonCodec
import io.circe.{Json, Printer}
import io.circe.parser._
import io.circe.literal._
import io.circe.syntax._
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.language.postfixOps

class CoinbaseMarketDataSource extends DataSource {

  import CoinbaseMarketDataSource._

  override def scheduleIngest(topics: Set[String], dataType: String) = {
    println("Scheduling")
    IngestGroup(topics, 0 seconds)
  }

//  override def ingest[T](topic: String, datatype: DataType[T])
//                        (implicit ctx: ActorContext, mat: ActorMaterializer) = {
//    datatype match {
//      case OrderBookType => ???
//    }
//  }

  override def ingestGroup[T](topics: Set[String], datatype: DataType[T])
                             (implicit ctx: ActorContext, mat: ActorMaterializer) = {

    val log = ctx.system.log

    val (jsonRef, src) = Source
      .actorRef[Json](Int.MaxValue, OverflowStrategy.fail)
      .preMaterialize()

    class FullChannelClient(uri: URI) extends WebSocketClient(uri) {
      override def onOpen(handshakedata: ServerHandshake) = {
        log.info("Coinbase WebSocket open")
      }

      override def onMessage(message: String) = {
        parse(message) match {
          case Left(err) =>
            log.error(err.underlying, "Parsing error in Coinbase Pro Websocket: {}", err.message)
            jsonRef ! PoisonPill
          case Right(jsonValue) =>
            jsonRef ! jsonValue
        }
      }

      override def onClose(code: Int, reason: String, remote: Boolean) = {
        log.info("Coinbase WebSocket closed")
        jsonRef ! PoisonPill
      }

      override def onError(ex: Exception) = {
        log.error(ex, "Exception in Coinbase Pro WebSocket. Shutting down the stream.")
        jsonRef ! PoisonPill
      }
    }

    val client = new FullChannelClient(new URI("wss://ws-feed.pro.coinbase.com"))

    val eventRefs = topics.map(_ -> {
      Source.actorRef[StreamItem](Int.MaxValue, OverflowStrategy.fail).preMaterialize()
    }).toMap

    val snapshotPromises = topics.map(_ -> Promise[StreamItem]).toMap

    // Complete this promise once we get a "subscribed" message.
    val subscribedPromise = Promise[Map[String, Source[(Long, T), NotUsed]]]

    Future {
      if (!client.connectBlocking(30, SECONDS)) {
        subscribedPromise.failure(new RuntimeException("Unable to connect to Coinbase Pro WebSocket"))
      }
    }(ExecutionContext.global)

    val msgJson = json"""
      {
        "type": "subscribe",
        "product_ids": $topics,
        "channels": "full"
      }
    """
    val strMsg = msgJson.pretty(Printer.noSpaces)
    client.send(strMsg)

    src
      // Ignore any events that come in before the "subscribed" event
      .dropWhile(eventType(_) != Subscribed)

      // Complete the promise as soon as we have a "subscribed" event
      .alsoTo(Sink.foreach { _ =>
        if (!subscribedPromise.isCompleted) {
          subscribedPromise.success(eventRefs.map {
            case (topic, (ref, eventSrc)) =>
              val snapshotSrc = Source.fromFuture(snapshotPromises(topic).future)
              val src = eventSrc
                .mergeSorted(snapshotSrc)(streamItemOrdering)
                .via(util.stream.deDupeBy(_.seq))
                .dropWhile(!_.isBook)
                .scan[Option[StreamItem]](None) {
                  case (None, item) if item.isBook => Some(item)
                  case (Some(book), item) if !item.isBook => Some(item.copy(data =
                    Left(book.book.processOrderEvent(item.event))))
                }.collect { case Some(item) => (item.micros, item.book.asInstanceOf[T]) }
              topic -> src
          })
        }
      })

      // Drop everything except for book events
      .filter(BookEventTypes contains eventType(_))

      // Map to StreamItem
      .map[StreamItem] { json =>
        val unparsed = json.as[UnparsedAPIOrderEvent].right.get
        val orderEvent = unparsed.toOrderEvent
        StreamItem(unparsed.sequence.get, unparsed.micros, Right(orderEvent))
      }

      // Send to event ref
      .runForeach { item => eventRefs(item.event.product)._1 ! item }

      // Shut down all event refs when stream completes.
      .onComplete { _ =>
        eventRefs.values.map(_._1).foreach(_ ! PoisonPill)
        try {
          client.close()
        } catch {
          case err: Throwable =>
            log.warning("An error occured while closing the Coinbase WebSocket connection: {}", err)
        }
      }(ctx.dispatcher)

    subscribedPromise.future
  }
}

object CoinbaseMarketDataSource {

  case class StreamItem(seq: Long, micros: Long, data: Either[OrderBook, OrderEvent]) {
    def isBook: Boolean = data.isLeft
    def book: OrderBook = data.left.get
    def event: OrderEvent = data.right.get
  }

  val streamItemOrdering: Ordering[StreamItem] = new Ordering[StreamItem] {
    override def compare(x: StreamItem, y: StreamItem): Int = {
      if (x.seq < y.seq) -1
      else if (x.seq > y.seq) 1
      else if (x.isBook && !y.isBook) -1
      else if (!x.isBook && y.isBook) 1
      else 0
    }
  }

  val Open = "open"
  val Done = "done"
  val Received = "received"
  val Change = "change"
  val Match = "match"
  val Subscribed = "subscribed"

  val BookEventTypes = List(Open, Done, Received, Change, Match)

  def eventType(json: Json): String =
    json.hcursor.get[String]("type").right.get

  // How we receive order events from the API. Fields are strings for some reason.
  @JsonCodec
  case class UnparsedAPIOrderEvent(`type`: String,
                                   product_id: String,
                                   sequence: Option[Long],
                                   time: Option[String],
                                   size: Option[String],
                                   price: Option[String],
                                   order_id: Option[String],
                                   side: Option[String],
                                   reason: Option[String],
                                   order_type: Option[String],
                                   remaining_size: Option[String],
                                   funds: Option[String],
                                   trade_id: Option[Long],
                                   maker_order_id: Option[String],
                                   taker_order_id: Option[String],
                                   taker_user_id: Option[String],
                                   user_id: Option[String],
                                   taker_profile_id: Option[String],
                                   profile_id: Option[String],
                                   new_size: Option[String],
                                   old_size: Option[String],
                                   new_funds: Option[String],
                                   old_funds: Option[String],
                                   last_size: Option[String],
                                   best_bid: Option[String],
                                   best_ask: Option[String],
                                   client_oid: Option[String]) {
    def toOrderEvent: OrderEvent = {
      `type` match {
        case Open =>
          OrderOpen(order_id.get, CurrencyPair(product_id), price.get.toDouble, remaining_size.get.toDouble,
            Side.parseSide(side.get))
        case Done =>
          OrderDone(order_id.get, CurrencyPair(product_id), Side.parseSide(side.get),
            DoneReason.parse(reason.get), price.map(_.toDouble), remaining_size.map(_.toDouble))
        case Change =>
          OrderChange(order_id.get, CurrencyPair(product_id), price.map(_.toDouble), new_size.get.toDouble)
        case Match =>
          OrderMatch(trade_id.get, CurrencyPair(product_id), micros, size.get.toDouble, price.get.toDouble,
            Side.parseSide(side.get), maker_order_id.get, taker_order_id.get)
        case Received =>
          OrderReceived(order_id.get, CurrencyPair(product_id), client_oid, OrderType.parseOrderType(order_type.get))
      }
    }

    def micros: Long = time.map(TimeFmt.ISO8601ToMicros).get
  }

}
