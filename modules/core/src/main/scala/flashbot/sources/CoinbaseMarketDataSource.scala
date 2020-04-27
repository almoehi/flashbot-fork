package flashbot.sources

import java.net.URI
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, ChronoUnit, TemporalAccessor}
import java.util.concurrent.{Executors, TimeUnit}

import akka.{Done, NotUsed}
import akka.actor.{ActorContext, ActorRef, PoisonPill}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
import flashbot.core.DataSource.IngestGroup
import flashbot.core.DataType.{CandlesType, LadderType, OrderBookType, TradesType}
import flashbot.core.Instrument.CurrencyPair
import flashbot.core._
import flashbot.util.time.TimeFmt
import flashbot.util.stream._
import flashbot.util
import io.circe.generic.JsonCodec
import io.circe.{Json, JsonObject, Printer}
import io.circe.parser._
import io.circe.literal._
import io.circe.syntax._
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import com.softwaremill.sttp._
import com.softwaremill.sttp.Uri.QueryFragment.KeyValue
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import flashbot.core.DataType
import flashbot.models.Order.{Buy, OrderType, Sell, TickDirection}
import flashbot.models.{Candle, OrderBook}
import flashbot.util.network.RequestService._

import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class CoinbaseMarketDataSource extends DataSource {

  import CoinbaseMarketDataSource._

  val products = debox.Map.empty[String,CoinbaseProduct]

  val defaultTickSize = 0.01

  private def fetchProducts()(implicit ctx: ActorContext, mat: ActorMaterializer) = {
    implicit val ec: ExecutionContext = ctx.dispatcher
    val log = ctx.system.log
    val timeout = 10 seconds
    var uri = uri"https://api.pro.coinbase.com/products"

    log.debug(s"Fetching /products infor from $uri")

    sttp.get(uri).sendWithRetries().flatMap { rsp =>
      rsp.body match {
        case Left(err) => Future.failed(new RuntimeException(s"Error in Coinbase /products request: $err"))
        case Right(bodyStr) => Future.fromTry(decode[Seq[CoinbaseProduct]](bodyStr).toTry)
      }
    } onComplete {
      case Success(xs) =>
        log.info(s"Loaded /products information $xs")
        xs.map{p =>
          products(CurrencyPair(p.id).symbol) = p
        }
      case Failure(err) =>
        log.error(err, "Failed to fetch /products information")
    }
  }

  private def subscribeMessageJson(channels: Set[String], cbProducts: Set[String]) = {
    import io.circe.syntax._

    val validChannels = channels.filter(c => CoinbaseMarketDataSource.ChannelTypes.contains(c)).toSeq

    Map("type" -> "subscribe".asJson, "channels" -> validChannels.map{
      case CoinbaseMarketDataSource.Status => Map("name" -> CoinbaseMarketDataSource.Status.asJson).asJson
      case other => Map("name" -> other.asJson, "product_ids" -> cbProducts.asJson).asJson
    }.asJson).asJson
  }

  override def scheduleIngest(topics: Set[String], dataType: String) = {
    IngestGroup(topics, 0 seconds)
  }

  override def types = Seq(TradesType, OrderBookType, CandlesType(1 minute))

  override def ingestGroup[T](topics: Set[String], datatype: DataType[T])
                             (implicit ctx: ActorContext, mat: ActorMaterializer) = {

    implicit val ec: ExecutionContext = ctx.dispatcher

    val log = ctx.system.log

    if (products.isEmpty) {
      fetchProducts()
    }


    log.debug("Starting ingest group {}, {}", topics, datatype)

    val (jsonRef, jsonSrc) = Source
      .actorRef[Json](Int.MaxValue, OverflowStrategy.fail)
      // Ignore any events that come in before the "subscriptions" event
      .dropWhile(eventType(_) != Subscribed)
      .preMaterialize()

    class FullChannelClient(uri: URI, retries: Int = 5) extends WebSocketClient(uri) {

      var subscribeMessages: Set[String] = Set.empty
      var subscribed: Map[String,Boolean] = Map.empty
      var attempts: Int = 0

      def subscribeTo(msg: String): Unit = {
        log.debug("Sending subscribe message: {}", msg)
        subscribeMessages = subscribeMessages + msg
        this.send(msg)
      }

      override def onOpen(handshakedata: ServerHandshake) = {
        log.info("Coinbase WebSocket open")

        // this is a re-connect - try to reestablish previous subscriptions
        if (attempts > 0 && attempts <= retries) {
          subscribeMessages.map(subscribeTo(_))
        }
      }

      override def onMessage(message: String) = {
        parse(message) match {
          case Left(err) =>
            log.error(err.underlying, "Parsing error in Coinbase Pro Websocket: {}", err.message)
            jsonRef ! PoisonPill
            /*
          case Right(jsonValue) if (eventType(jsonValue) == Heartbeat) =>
            // TODO: check for missed messages via last_trade_id and sequence ???
            val hb = jsonValue.as[CoinbaseHeartbeat].right.get
            //log.info("Coinbase Pro Websocket hearbeat {}", hb)
            jsonRef ! jsonValue
          case Right(jsonValue) if (eventType(jsonValue) == Error) =>
            val errorObj = jsonValue.as[CoinbaseError].right.get
            log.error("Coinbase Pro Websocket error {}", errorObj)
            jsonRef ! jsonValue
             */
          case Right(jsonValue) => eventType(jsonValue) match {
              case Error =>
                val errorObj = jsonValue.as[CoinbaseError].right.get
                log.error("Coinbase Pro Websocket error {}", errorObj)
              case Heartbeat =>
              case Subscribed =>
                val obj = jsonValue.as[CoinbaseSubscriptions].right.get
                subscribed = subscribed ++ obj.channels.map(c => (c.name,true)).toMap
                log.info("Coinbase Pro Websocket subscribed to {}", obj)
              case _ => // Ignore
            }

            if (attempts > 0 && subscribed.size == subscribeMessages.size) {
              attempts = 0
            }

            /*
            if (subscribed.size != subscribeMessages) {
              (subscribeMessages -- subscribed).map(s => subscribeTo(subscribeMessages(s)))
            }
             */

            jsonRef ! jsonValue
        }
      }

      override def onClose(code: Int, reason: String, remote: Boolean) = {
        log.info("Coinbase WebSocket closed  code={} reason={}, remote={} - reconnecting ...", code, reason, remote)
        subscribed = Map.empty

        if (attempts > retries) {
          jsonRef ! PoisonPill
          throw new RuntimeException(s"Coinbase WebSocket closed  code=$code reason=$reason remote=$remote")
        }

        // try to reconnect
        attempts += 1
        Future(blocking {
          reconnect()
        })
      }

      override def onError(ex: Exception) = {
        log.error(ex, "Exception in Coinbase Pro WebSocket. Trying to reconnect ...")
        subscribed = Map.empty

        if (attempts > retries) {
          log.error(ex, "Exception in Coinbase Pro WebSocket. Max. retries {} reached, shutting down the stream.", attempts)
          jsonRef ! PoisonPill
          throw ex
        }

        // try to reconnect
        attempts += 1
        Future(blocking {
          reconnect()
        })
      }
    }

    val client = new FullChannelClient(new URI("wss://ws-feed.pro.coinbase.com"))

    // Complete this promise once we series a "subscriptions" message.
    val responsePromise = Promise[Map[String, Source[(Long, T), NotUsed]]]

    // Events are sent here as StreamItem instances
    val eventRefs = topics.map(_ ->
      Source.actorRef[StreamItem[_]](Int.MaxValue, OverflowStrategy.fail).preMaterialize()).toMap

    datatype match {
      case OrderBookType =>
        // Asynchronously connect to the client and send the subscription message
        Future(blocking {
          if (!client.connectBlocking(30, SECONDS)) {
            responsePromise.failure(new RuntimeException("Unable to connect to Coinbase Pro WebSocket"))
          }
          val cbProducts: Set[String] = topics.map(toCBProduct)
          val strMsg = subscribeMessageJson(Set(CoinbaseMarketDataSource.Full), cbProducts).noSpaces
          /*
          val strMsg = s"""
            {
              "type": "subscribe",
              "channels": [{"name": "full", "product_ids": ${cbProducts.asJson.noSpaces}}]
            }
          """
           */
          // Send the subscription message
          client.subscribeTo(strMsg)
        })

        val snapshotPromises = topics.map(_ -> Promise[StreamItem[_]]).toMap

        jsonSrc
          .filterNot(Heartbeat == eventType(_))
          .alsoTo(Sink.foreach { _ =>
          if (!responsePromise.isCompleted) {
            // Complete the promise as soon as we have a "subscriptions" event
            responsePromise.success(eventRefs.map {
              case (topic, (ref, eventSrc)) =>
                val snapshotSrc = Source.fromFuture(snapshotPromises(topic).future)
                val (done, src: Source[(Long, T), NotUsed]) = eventSrc
                  .mergeSorted(snapshotSrc)(streamItemOrdering)
                  .via(util.stream.deDupeBy(_.seq))
                  .dropWhile(!_.isBook)
                  .scan[Option[StreamItem[_]]](None) {
                    case (None, item) if item.isBook => Some(item)
                    case (Some(memo), item) if !item.isBook => Some(
                      try {
                        item.copy(data = OrderBook.Delta.fromOrderEventOpt(item.event).foldLeft(memo.book)(_ update _))
                      } catch {
                        case t:Throwable =>
                          log.error(t, "Failed to update OrderBook using event {}", item.event)
                          item.copy(data = memo.book)
                      }
                    )
                  }
                  .collect { case Some(item) if item.micros != -1 => (item.micros, item.book.asInstanceOf[T]) }
                  .watchTermination()(Keep.right).preMaterialize()
                done.onComplete(_ => {
                  ref ! PoisonPill
                })
                topic -> src
            })

            // Also kick off the snapshot requests.
            for (topic <- eventRefs.keySet) {
              val product = toCBProduct(topic)
              var uri = uri"https://api.pro.coinbase.com/products/$product/book?level=3"
              val snapRef = snapshotPromises(topic)
              sttp.get(uri).sendWithRetries().flatMap { rsp =>
                rsp.body match {
                  case Left(err) => Future.failed(new RuntimeException(s"Error in Coinbase snapshot request: $err"))
                  case Right(bodyStr) => Future.fromTry(decode[BookSnapshot](bodyStr).toTry)
                }
              } onComplete {
                case Success(snapshot) =>
                  val tickSize = products.get(topic).map(_.tickSize).getOrElse(defaultTickSize)
                  // TODO: where to get micros here ?
                  snapRef.success(StreamItem[OrderBook](snapshot.sequence, -1, snapshot.toOrderBook(tickSize)))
                case Failure(err) =>
                  snapRef.failure(err)
              }
            }
          }
        })

        // Drop everything except for book events
        .filter(BookEventTypes contains eventType(_))
        .filter{json =>
          json.hcursor.get[String]("order_id").toOption.filter(_.nonEmpty) match {
            case Some(id) if eventType(json) != Match => true
            case _ if eventType(json) != Match =>
              log.warning(s"OrderBook event without order_id {}", json)
              false
            case _ => true
          }
        } // coinbase sometimes sends order event without or empty order_id
        // Map to StreamItem
        .map[StreamItem[OrderEvent]] { json =>
          val unparsed = json.as[UnparsedAPIOrderEvent].right.get
          val orderEvent = unparsed.toOrderEvent
          StreamItem(unparsed.sequence.get, unparsed.micros, orderEvent)
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
        }

      case TradesType =>
        // Asynchronously connect to the client and send the subscription message
        Future(blocking {
          if (!client.connectBlocking(30, SECONDS)) {
            responsePromise.failure(new RuntimeException("Unable to connect to Coinbase Pro WebSocket"))
          }
          val cbProducts: Set[String] = topics.map(toCBProduct)
          val strMsg = subscribeMessageJson(Set(CoinbaseMarketDataSource.Matches, CoinbaseMarketDataSource.Heartbeat), cbProducts).noSpaces
          /*
          val strMsg = s"""
            {
              "type": "subscribe",
              "channels": [{"name": "matches", "product_ids": ${cbProducts.asJson.noSpaces}}]
            }
          """
           */
          // Send the subscription message
          client.subscribeTo(strMsg)
        })

        jsonSrc
          .filterNot(Heartbeat == eventType(_))
          .alsoTo(Sink.foreach { json =>
          // Resolve promise if necessary
          if (!responsePromise.isCompleted) {
            responsePromise.success(eventRefs.map {
              case (topic, (ref, eventSrc)) =>
                val (done, src) = eventSrc
                  .map {
                    case StreamItem(seq, micros, om: OrderMatch) =>
                      (micros, om.toTrade.asInstanceOf[T])
                  }
                  .watchTermination()(Keep.right)
                  .preMaterialize()

                done.onComplete(_ => {
                  ref ! PoisonPill
                })
                topic -> src
            })
          }
        })

        // Drop everything except for book events
        .filter(BookEventTypes contains eventType(_))

        // Map to StreamItem
        .map[StreamItem[OrderEvent]] { json =>
          val unparsed = json.as[UnparsedAPIOrderEvent].right.get
          val orderEvent = unparsed.toOrderEvent
          StreamItem(unparsed.sequence.get, unparsed.micros, orderEvent)
        }

        // Send to event ref
        .runForeach { item => eventRefs(item.event.product)._1 ! item }

        .onComplete { _ =>
          eventRefs.values.map(_._1).foreach(_ ! PoisonPill)
          try {
            client.close()
          } catch {
            case err: Throwable =>
              log.warning("An error occured while closing the Coinbase WebSocket connection: {}", err)
          }
        }

      case CandlesType(d: FiniteDuration) if d == 1.minute =>
        responsePromise.completeWith(for {
          srcMap <- ingestGroup(topics, TradesType).map(_.map {
            case (topic, src) =>
              topic -> src.map(_._2)
                .map(t => (t.instant, t.price, t.size))
                .via(PriceTap.aggregateTradesFlow(d).map(c => (c.micros, c.asInstanceOf[T])))
          })
          (keys, sources) = srcMap.toSeq.unzip
          delayedSources <- Future.sequence(sources.map(waitForFirstItem))
        } yield keys.zip(delayedSources).toMap)
    }

    responsePromise.future
  }

  override def backfillPage[T](topic: String, datatype: DataType[T], cursorStr: Option[String])
                              (implicit ctx: ActorContext, mat: ActorMaterializer)
      : Future[(Vector[(Long, T)], Option[(String, FiniteDuration)])] = datatype match {
    case TradesType =>
      implicit val ec: ExecutionContext = ctx.dispatcher
      val cursor = cursorStr.map(decode[BackfillCursor](_).right.get)
      val product = toCBProduct(topic)
      var uri = uri"https://api.pro.coinbase.com/products/$product/trades"
      if (cursor.isDefined) {
        uri = uri.queryFragment(KeyValue("after", cursor.get.cbAfter))
      }

      sttp.get(uri).sendWithRetries().flatMap { rsp =>
        val nextCbAfterOpt = rsp.headers.toMap.get("cb-after").filterNot(_.isEmpty)
        rsp.body match {
          case Left(err) =>
            Future.failed(new RuntimeException(s"Error in Coinbase backfill: $err"))
          case Right(bodyStr) => Future.fromTry(decode[Seq[CoinbaseTrade]](bodyStr).toTry)

            // When you reach the end, it looks like they just return a list of the same trade.
            .map(_.toStream.dropDuplicates(Ordering.by(_.trade_id)).toVector)

            // Filter out any overlapping trades with prev page.
            .map(_.dropWhile(_.trade_id >=
              cursor.map(_.lastItemId.toLong).getOrElse[Long](Long.MaxValue)))

            // Map to page response.
            .map { trades =>
              val nextCursorOpt = for {
                nextCbAfter <- nextCbAfterOpt
                // This sets the cursor to None if `trades` is empty.
                lastTrade <- trades.lastOption
              } yield BackfillCursor(nextCbAfter, lastTrade.trade_id.toString)

              (trades.map(_.toTrade).map(t => (t.micros, t.asInstanceOf[T])),
                nextCursorOpt.map(x => (x.asJson.noSpaces, 4 seconds)))
            }
        }
      }

    case CandlesType(d) if d == 1.minute =>
      implicit val ec: ExecutionContext = ctx.dispatcher
      val product = toCBProduct(topic)
      val now = Instant.now()
      val endInstant = cursorStr.map(s =>
          Instant.from(DateTimeFormatter.ISO_INSTANT.parse(s)))
        .getOrElse(now)
      val end = DateTimeFormatter.ISO_INSTANT.format(endInstant)
      val start = DateTimeFormatter.ISO_INSTANT.format(endInstant.minusSeconds(60 * 200))
      val uri = uri"https://api.pro.coinbase.com/products/$product/candles?start=$start&end=$end&granularity=60"
      sttp.get(uri).sendWithRetries().flatMap { rsp =>
        rsp.body match {
          case Left(err) =>
            Future.failed(new RuntimeException(err))
          case Right(value) =>
            Future.fromTry(decode[Seq[(Long, Double, Double, Double, Double, Double)]](value).toTry)
              .map(rawData => {
                val data = rawData.toVector.map {
                  case (secs, low, high, open, close, volume) =>
                    val micros = secs * 1000000
                    (micros, Candle(micros, open, high, low, close, volume).asInstanceOf[T])
                }
                val cursor =
                  if (endInstant.isBefore(now.minus(4 * 365, ChronoUnit.DAYS))) None
                  else Some((start, 4 seconds))
                (data, cursor)
              })
        }
      }

    case _ => Future.failed(new UnsupportedOperationException(
      s"Coinbase backfill not implemented for $datatype."))
  }
}

object CoinbaseMarketDataSource {

  def toCBProduct(pair: String): String = pair.toUpperCase.replace("_", "-")

  trait WithSequence {
    val seq: Long
  }

  //Either[OrderBook, OrderEvent]
  case class StreamItem[T](seq: Long, micros: Long, data: T) extends WithSequence {
    def isBook: Boolean = data.isInstanceOf[OrderBook]
    def isOrder: Boolean = data.isInstanceOf[OrderEvent]
    def isHeartbeat: Boolean = data.isInstanceOf[CoinbaseHeartbeat]
    def book: OrderBook = data.asInstanceOf[OrderBook]
    def event: OrderEvent = data.asInstanceOf[OrderEvent]
    def heartbeat: CoinbaseHeartbeat = data.asInstanceOf[CoinbaseHeartbeat]
  }

  val streamItemOrdering: Ordering[StreamItem[_]] = new Ordering[StreamItem[_]] {
    override def compare(x: StreamItem[_], y: StreamItem[_]): Int = {
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
  val Subscribed = "subscriptions"
  val Error = "error"

  // valid Coinbase Websocket channels to subscribe to
  val Heartbeat = "heartbeat"
  val Status = "status"
  val Trades = "trades"
  val Matches = "matches"
  val Full = "full"
  val Ticker = "ticker"

  val BookEventTypes = List(Open, Done, Received, Change, Match)

  val ChannelTypes = List(Heartbeat, Status, Trades, Matches, Full, Ticker)

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
          // TODO: add order-type (market, price==null or limit price = Some())
          OrderOpen(order_id.get, CurrencyPair(product_id), price.get.toDouble, remaining_size.get.toDouble, side.get)
        case Done =>
          OrderDone(order_id.get, CurrencyPair(product_id), side.get,
            DoneReason.parse(reason.get), price.flatMap(d => Try(d.toDouble).toOption), remaining_size.flatMap(d => Try(d.toDouble).toOption))
        case Change =>
          // TODO: add order-type (market, price==null or limit price = Some())
          OrderChange(order_id.get, CurrencyPair(product_id), price.flatMap(d => Try(d.toDouble).toOption), new_size.get.toDouble)
        case Match =>
          OrderMatch(trade_id.get.toString, CurrencyPair(product_id), micros, size.get.toDouble, price.get.toDouble,
            TickDirection.ofMakerSide(side.get), maker_order_id.get, taker_order_id.get)
        case Received =>
          // TODO: add 'funds' field
          OrderReceived(order_id.get, CurrencyPair(product_id), client_oid.get, OrderType.parseOrderType(order_type.get))
      }
    }

    def micros: Long = time.map(TimeFmt.ISO8601ToMicros).get
  }

  /**
    * {
    *   "time": "2014-11-07T22:19:28.578544Z",
    *   "trade_id": 74,
    *   "price": "10.00000000",
    *   "size": "0.01000000",
    *   "side": "buy"
    * }
    */
  @JsonCodec case class CoinbaseTrade(time: String, trade_id: Long, price: String, size: String, side: String) {
    implicit def toTrade: Trade = Trade(trade_id.toString,
      TimeFmt.ISO8601ToMicros(time), price.toDouble, size.toDouble, TickDirection.ofMakerSide(side))
  }

  @JsonCodec case class CoinbaseError(`type`: String, message: String)

  @JsonCodec case class CoinbaseChannel(name: String, product_ids: Seq[String])

  @JsonCodec case class CoinbaseSubscriptions(`type`: String, channels: Seq[CoinbaseChannel])

  /*
      {
        "id": "BTC-USD",
        "base_currency": "BTC",
        "quote_currency": "USD",
        "base_min_size": "0.001",
        "base_max_size": "10000.00",
        "quote_increment": "0.01" ==> tickSize
    }
   */
  @JsonCodec case class CoinbaseProduct(id: String, base_currency: String, quote_currency: String, base_min_size: Double, base_max_size: Double, quote_increment: Double) {
    lazy val tickSize = quote_increment
  }

  @JsonCodec case class CoinbaseHeartbeat(`type`: String, sequence: Long, last_trade_id: Long, product_id: String, time: String) {
    implicit def toStreamItem: StreamItem[CoinbaseHeartbeat] = StreamItem(sequence, TimeFmt.ISO8601ToMicros(time), this)
    lazy val micros = TimeFmt.ISO8601ToMicros(time)
    lazy val product = CurrencyPair(product_id)
  }

  @JsonCodec case class BackfillCursor(cbAfter: String, lastItemId: String)


  @JsonCodec case class BookSnapshot(sequence: Long,
                                     asks: Seq[(String, String, String)],
                                     bids: Seq[(String, String, String)]) {
    def toOrderBook(tickSize: Double): OrderBook = {
      val withAsks = asks.foldLeft(new OrderBook(tickSize)) {
        case (book, askSeq) => book.open(askSeq._3, askSeq._1.toDouble, askSeq._2.toDouble, Sell)
      }
      bids.foldLeft(withAsks) {
        case (book, askSeq) => book.open(askSeq._3, askSeq._1.toDouble, askSeq._2.toDouble, Buy)
      }
    }
  }
}
