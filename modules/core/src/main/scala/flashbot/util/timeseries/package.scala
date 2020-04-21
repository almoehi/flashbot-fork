package flashbot.util

import java.time
import java.time.{Instant, ZoneId, ZoneOffset}
import java.util.function

import cats.{Foldable, Monoid}
import com.fasterxml.jackson.databind.`type`.CollectionType
import flashbot.core.Timestamped.HasTime
import flashbot.models.Candle
import org.ta4j.core.{Bar, BaseBar, BaseTimeSeries, Indicator, TimeSeries}
import org.ta4j.core.BaseTimeSeries.SeriesBuilder
import org.ta4j.core.num.{DoubleNum, Num}
import flashbot.util.time._
import flashbot.util.timeseries.Implicits._
import flashbot.util.timeseries.Scannable
import flashbot.util.timeseries.Scannable.BaseScannable.ScannableInto

import scala.collection.{AbstractIterator, GenIterable, IterableLike, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

package object timeseries {

  protected[timeseries] class SeriesConfig(val outOfOrderDataIsError: Boolean,
                                           val maxBarCount: Int) {
    // Implementation details for bookkeeping of candles
    protected[timeseries] var baseInterval: Long = -1
    protected[timeseries] var lastCandleSeenAt: Long = -1
    protected[timeseries] var firstCandleBuffer: Option[Candle] = None
    protected[timeseries] var inferredCandleInterval: Long = -1

    def interval: java.time.Duration = intervalMicros micros
    def intervalMicros: Long = {
      if (baseInterval != -1) baseInterval
      else if (inferredCandleInterval != -1) inferredCandleInterval
      else throw new RuntimeException("This time series is not configured with a bar interval.")
    }

    def hasInterval: Boolean = baseInterval != -1 || inferredCandleInterval != -1

    def withInterval(newInterval: java.time.Duration): Unit = {
      val newBaseMicros = newInterval.toMicros
      SeriesConfig.validateIntervals(newBaseMicros, inferredCandleInterval)
      baseInterval = newBaseMicros
    }

    protected[timeseries] def registerCandle(micros: Long, open: Double, high: Double,
                                             low: Double, close: Double, volume: Double): Option[Candle] = {
      if (volume > 1700) {
        println("err")
      }
      var ejectedBufferedCandle: Option[Candle] = None
      if (lastCandleSeenAt == -1) {
        firstCandleBuffer = Some(Candle(micros, open, high, low, close, volume))
      } else {
        val inferred = micros - lastCandleSeenAt
//        println(inferred.micros.toCoarsest,
//          micros, micros.microsToZdtLocal)

        // Make sure that all candles are evenly spaced. A.k.a. the inferred interval should
        // always be constant from candle to candle.
        if (inferredCandleInterval != -1 && inferred != inferredCandleInterval)
          throw new RuntimeException(
            s"""
              |The inferred interval from the latest candles, ${inferred.micros.toCoarsest},
              |does not match the previously inferred interval of ${inferredCandleInterval.micros.toCoarsest}.
              |""".stripMargin)

        // Also validate against the base interval.
        SeriesConfig.validateIntervals(baseInterval, inferred)

        // Then set the inferred
        inferredCandleInterval = inferred

        ejectedBufferedCandle = firstCandleBuffer
        firstCandleBuffer = None
      }
      lastCandleSeenAt = micros
      ejectedBufferedCandle
    }
  }

  object SeriesConfig {
    def apply(outOfOrderDataIsError: Boolean = false,
              maxBarCount: Int = 10000): SeriesConfig =
      new SeriesConfig(outOfOrderDataIsError, maxBarCount)

    private def validateIntervals(base: Long, inferred: Long): Unit = {
      if (inferred != -1 && base != -1 && base < inferred) {
        throw new RuntimeException(
          s"""Base time series interval of ${base.micros.toCoarsest} must be larger than
             |the interval of ${inferred.micros.toCoarsest} which was inferred from candles.""".stripMargin)
      }
    }
  }

  def timeSeriesIterator(series: TimeSeries): Iterator[Bar] = series.iterator

  object Implicits {
    implicit class TimeSeriesOps(series: TimeSeries) {
      // This must not use `synchronized`, otherwise there will be a deadlock
      // in `updateConfig`
      def config: SeriesConfig = seriesConfigs(series)
      def getPreviousBar: Bar = series.getBar(series.getEndIndex - 1)
      def withInterval(interval: java.time.Duration): TimeSeries = {
        series.config.withInterval(interval)
        series
      }
      def interval: java.time.Duration = series.config.interval
      def hasInterval: Boolean = series.config.hasInterval

      def put(candle: Candle): TimeSeries = putCandle(series, candle)

      def iterator: Iterator[Bar] = new Iterator[Bar] {
        private var i = series.getBeginIndex
        override def hasNext: Boolean = i <= series.getEndIndex
        override def next(): Bar = {
          val bar = series.getBar(i)
          i += 1
          bar
        }
      }
    }

    implicit class BarOps(bar: Bar) {
      def open: Double = bar.getOpenPrice.getDelegate.doubleValue()
      def high: Double = bar.getMaxPrice.getDelegate.doubleValue()
      def low: Double = bar.getMinPrice.getDelegate.doubleValue()
      def close: Double = bar.getClosePrice.getDelegate.doubleValue()
      def volume: Double = bar.getVolume.getDelegate.doubleValue()
      def micros: Long = bar.getBeginTime.millis * 1000
      def candle: Candle = Candle(micros, open, high, low, close, volume)

      def buildCandleConverter = (candle: Candle) => {
        val fn = bar.getAmount.function()
        new BaseBar(bar.getTimePeriod,
          candle.instant.zdt.plus(bar.getTimePeriod),
          fn(candle.open),
          fn(candle.high),
          fn(candle.low),
          fn(candle.close),
          fn(candle.volume),
          fn(0)
        )
      }
    }

//    implicit class CandleOps(candle: Candle) {
//      def bar: Bar = {
//
//      }
//    }

    implicit class StringOps(name: String) {
      def timeSeries: TimeSeries = buildTimeSeries(name)
      def timeSeries(config: SeriesConfig): TimeSeries = buildTimeSeries(name, config)
    }

    implicit class IndicatorOps[I <: Indicator[_]](indicator: I) {

    }
  }

  def buildTimeSeries(name: String): TimeSeries = buildTimeSeries(name, SeriesConfig())

  def buildTimeSeries(name: String, config: SeriesConfig): TimeSeries = {
    val ts = new BaseTimeSeries.SeriesBuilder()
      .withName(name)
      .withMaxBarCount(config.maxBarCount)
      .withNumTypeOf(DoubleNum.valueOf(_))
      .build()
    putConfig(ts, config)
    ts
  }

  def putCandle(series: TimeSeries, candle: Candle): TimeSeries =
    putOHLCV(series, candle.micros, candle.open, candle.high, candle.low, candle.close, candle.volume)

  private def putCandle(series: TimeSeries, candle: Candle, shouldRegister: Boolean): TimeSeries =
    putOHLCV(series, candle.micros, candle.open, candle.high, candle.low,
      candle.close, candle.volume, shouldRegister)

  def putOHLCV(series: TimeSeries, micros: Long, open: Double, high: Double, low: Double,
               close: Double, volume: Double): TimeSeries =
    putOHLCV(series, micros, open, high, low, close, volume, shouldRegister = true)

  private def putOHLCV(series: TimeSeries, micros: Long, open: Double, high: Double, low: Double,
                       close: Double, volume: Double, shouldRegister: Boolean): TimeSeries = {
    if (shouldRegister) {
      // Register the incoming candle with the config. Iff this is the second candle to register,
      // the function will eject the first candle, which should be placed into the series before
      // the current one. The ejected one should not register again.
      val ejectedBufferedCandle = series.config.registerCandle(micros, open, high, low, close, volume)
      if (ejectedBufferedCandle.isDefined)
        putCandle(series, ejectedBufferedCandle.get, shouldRegister = false)
    }

    if (series.hasInterval) {
      put(series, micros, (_, isNewBar) => {
        val curBar = series.getLastBar
        val fn = series.function
        val newOpen = if (isNewBar) fn(open) else curBar.getOpenPrice
        val newHigh = if (isNewBar) fn(high) else curBar.getMaxPrice.max(fn(high))
        val newLow = if (isNewBar) fn(low) else curBar.getMinPrice.min(fn(low))
        val newClose = fn(close)
        val newVolume = curBar.getVolume.plus(fn(volume))
        val newBar = new BaseBar(curBar.getTimePeriod, curBar.getEndTime,
          newOpen, newHigh, newLow, newClose, newVolume, fn(0))
        series.addBar(newBar, true)
      })
    }

    series
  }

  private def put(series: TimeSeries, micros: Long,
                  updateLastBar: (TimeSeries, Boolean) => Unit): TimeSeries = {
    val config = series.config
    val intervalMicros = config.intervalMicros
    if (intervalMicros % 1000 != 0)
      throw new RuntimeException("TimeSeries intervals must have millisecond granularity")
    val intervalMillis = config.intervalMicros / 1000
    val globalIndex = micros / (intervalMillis * 1000)
    val alignedMillis = globalIndex * intervalMillis
    val zdt = Instant.ofEpochMilli(alignedMillis).zdt

    // If the data is outdated, then either throw or ignore and return immediately.
    if (series.getBarCount > 0 && series.getLastBar.getBeginTime.isAfter(zdt)) {
      if (config.outOfOrderDataIsError)
        throw new RuntimeException("""This time series does not support outdated data.""")
      else
        return series
    }

    // Until the last bar exists and accepts the current time, create a new bar.
    var addedBars: Int = 0
    while (series.getBarCount == 0 || !series.getLastBar.inPeriod(zdt)) {
      val lastBar: Option[Bar] = if (series.getBarCount == 0) None else Some(series.getLastBar)
      val startingTime = if (lastBar.isEmpty) zdt else lastBar.get.getEndTime

      // But, before we add the new bar, we make sure the last one isn't empty. If it empty,
      // copy over the close data from the one before it so that calculations aren't messed up.
      if (lastBar.isDefined && addedBars > 0) {
        // The second to last bar should always exist if an empty last bar exists.
        // Furthermore, it should never be empty.
        lastBar.get.addPrice(series.getPreviousBar.getClosePrice)
      }

      // Ok, now we can add the new bar.
      val interval = config.interval
      series.addBar(interval, startingTime.plus(interval))
      addedBars = addedBars + 1
    }

    updateLastBar(series, addedBars > 0)

    series
  }

  private val seriesConfigs = mutable.WeakHashMap.empty[TimeSeries, SeriesConfig]
  private def putConfig(series: TimeSeries, config: SeriesConfig): TimeSeries = {
    seriesConfigs.synchronized {
      seriesConfigs(series) = config
    }
    series
  }
  private def updateConfig(series: TimeSeries, updateFn: SeriesConfig => SeriesConfig): TimeSeries = {
    seriesConfigs.synchronized {
      seriesConfigs(series) = updateFn(series.config)
    }
    series
  }

  def scan[I:HasTime, C:HasTime](items: Iterable[I],
                                 timeStep: java.time.Duration,
                                 dropFirst: Boolean = false,
                                 dropLast: Boolean = false)
                                (implicit scanner: ScannableInto[I, C],
                                 CollectionType: TimeSeriesFactory[C]): TimeSeriesLike[C] =
    scanVia(items, timeStep, CollectionType, dropFirst, dropLast)

  def scanVia[I:HasTime, C:HasTime](itemsI: Iterable[I],
                                    timeStep: java.time.Duration,
                                    fact: TimeSeriesFactory[C],
                                    dropFirst: Boolean = false,
                                    dropLast: Boolean = false)
                                   (implicit scanner: ScannableInto[I, C]): TimeSeriesLike[C] = {

    val items = itemsI.iterator

    def getItemMicros(item: I): Long = HasTime[I].micros(item)
    def candleIndex(item: I): Long = timeStep.timeStepIndexOfMicros(getItemMicros(item))

    // This iterator is sparse in the sense that it's gaps haven't been filled in yet.
    // That will happen at the time of scanning.
    val groupIterator = new Iterator[(Long, Iterator[I])] {
      var nextGroup: Iterator[I] = _
      var rest: Iterator[I] = items
      var bothSubGroupsNonEmpty: Boolean = _

      private def split(): Long = {
        var currentCandleIndex: Long = -1
        val (_nextGroup, _rest) = rest.span { item =>
          val myCandleIndex = candleIndex(item)
          if (currentCandleIndex == -1)
            currentCandleIndex = myCandleIndex
          currentCandleIndex == myCandleIndex
        }
        nextGroup = _nextGroup
        rest = _rest
        bothSubGroupsNonEmpty = nextGroup.nonEmpty && rest.nonEmpty
        currentCandleIndex
      }

      var groupIndex = split()

      override def hasNext =
        if (!dropLast) nextGroup.nonEmpty
        else bothSubGroupsNonEmpty

      override def next() = {
        val groupIt = nextGroup
        val prevIndex = groupIndex
        groupIndex = split()
        (prevIndex, groupIt)
      }
    }.drop(if (dropFirst) 1 else 0)/*
      .drop(if (dropFirst) 1 else 0).scanLeft[Option[(Long, C)]](None) {
      // First group
      case (None, (nextCandleIndex, nextItems)) =>
        val initialCandle: C = scanner.empty(timeStep.toMicros * nextCandleIndex, timeStep, None)
        val calcedCandle = nextItems.foldLeft[C](initialCandle)(scanner.fold)
        Some((nextCandleIndex, calcedCandle))

      // Next group. i.e. nextIndex = prevIndex + 1
      case (Some((prevIndex, prevCandle)), (nextIndex, nextItems)) if prevIndex + 1 == nextIndex =>
        val initialCandle: C = scanner.empty(timeStep.toMicros * nextIndex, timeStep, Some(prevCandle))
        Some((nextIndex, nextItems.foldLeft[C](initialCandle)(scanner.fold)))

      // Detected missing time steps. Fill them in.
      case (Some((prevIndex, prevCandle)), (nextIndex, _)) if prevIndex + 1 < nextIndex =>
        val micros = timeStep.toMicros * (prevIndex + 1)
        Some((prevIndex + 1, scanner.empty(micros, timeStep, Some(prevCandle))))

    }.drop(1).map(_.get)//.map(_.get._2)
    */

    //println(s"GroupIterator size: ${groupIterator.size}")
    //println(s"GroupIterator")
    //spareIt.toList.map(el => (el._1,el._2.size)).map(println)

    val finalIt: Iterator[C] = new Iterator[C] {
      var bufferedNext: Long = -1
      var bufferedIt: Iterator[I] = _ // null
      var lastIdx: Long = -1
      var lastItem: Option[C] = None

      private def isCaughtUp: Boolean = lastIdx == bufferedNext && !bufferedIt.hasNext

      private def fetchNextIt(): Unit = {
        val x = groupIterator.next()
        bufferedNext = x._1
        bufferedIt = x._2
      }

      override def hasNext: Boolean = {
        // Initialize by buffering the first iterator
        if (bufferedIt == null) {
          if (groupIterator.hasNext) {
            fetchNextIt()
          } else {
            return false
          }
        }

        // If we have no more items to iterate on the current buffered it, get the next one.
        else if (isCaughtUp) {
          if (groupIterator.hasNext) {
            fetchNextIt()
          } else {
            return false
          }
        }

        true
      }

      override def next() = {
        if (lastIdx == -1) {
          val item = bufferedIt.next()
          val init = scanner.empty(timeStep.toMicros * bufferedNext, timeStep, None)
          lastItem = Some(scanner.fold(init, item))
          lastIdx = bufferedNext
        } else {
          val timeStepLag = bufferedNext - lastIdx
          if (timeStepLag == 0) {
            lastItem = lastItem.map(c => scanner.fold(c, bufferedIt.next()))
          } else if (timeStepLag == 1) {
            val item = bufferedIt.next()
            val init = scanner.empty(timeStep.toMicros * bufferedNext, timeStep, lastItem)
            lastItem = Some(scanner.fold(init, item))
            lastIdx = bufferedNext
          } else {
            //println(s"lastIdx=$lastIdx | bufferedNext=$bufferedNext | timeStepLag=$timeStepLag")
            lastItem = Some(scanner.empty(timeStep.toMicros * (lastIdx + 1), timeStep, lastItem))
            lastIdx = lastIdx + 1
          }
        }
        lastItem.get
      }
    }

    fact.fromTickingIterator(finalIt, timeStep)


//    var candleIterator = null
//
//    lazy val ret = fact.fromTickingIterator(candleIterator, timeStep)
//
//    def updatingFold(fn: (C, I) => C): (C, I) => C = (m, a) => {
//      ret.notify(m)
//      fn(m, a)
//    }
//
//    def unfoldItems(init: Option[C], micros: Long, items: Iterator[I]) =
//      items.scanLeft(scanner.empty(micros, timeStep, init)) {
//        case (in, item) => scanner.fold(in, item)
//      }
//
//    val evenlySpaced = unfoldIterator[
//      Iterator[(Long, Option[C] => Iterator[C])],
//      (Long, Iterator[(Long, Iterator[I])])
//    ]((-1L, groupIterator))(
//      memo => {
//        val (prevIndex, gi) = memo
//        if (gi.hasNext) {
//          val (nextIndex, next) = gi.next()
//          val realNextMicros = timeStep.toMicros * nextIndex
//          val missingIndexCount = if (prevIndex == -1) 0 else nextIndex - (prevIndex + 1)
//          val ret = (0L to missingIndexCount).reverse.map { i =>
//            val fn = (lastCandle: Option[C]) =>
//              unfoldItems(
//                lastCandle,
//                realNextMicros - i * timeStep.toMicros,
//                if (i == 0) next else Iterator.empty
//              )
//            (nextIndex - i, fn)
//          }.iterator
//          Some((ret, (nextIndex, gi)))
//        } else None
//      }).flatten
//
////    evenlySpaced.foldLeft[Option[C]](None) {
////      case (memo, (idx, fn2: ((Option[C]) => Iterator[C]))) => fn2(memo)
////    }
//
//    val fooo: Iterator[(Int, Long, I)] = evenlySpaced.flatMap(x => x._1._2.map(y => (x._2, x._1._1, y)))
//    fooo.scanLeft[Option[C]](None) {
//      case (None, (i, idx, item)) =>
//        val initialCandle: C = scanner.empty(timeStep.toMicros * idx, timeStep, None)
//        val inner = nextItems.scanLeft[C](initialCandle)(updatingFold(scanner.fold))
//        Some((nextCandleIndex, inner))
//    }
//
//    var fooIt = unfoldIterator[C, (Option[C], Option[Iterator[C]], Iterator[Iterator[I]])]((None, None, evenlySpaced)) {
//      case (_, _, its) if !its.hasNext => None
//      case (None, None, its) =>
//        val nextIt = its.next()
////      case (None, Some(its)) if its.hasNext => Some
//    }
//
//    //    unfoldIterator[Iterator[I], Iterator[(Long, Iterator[I])]](groupIterator) {
//    //      gi: Iterator[(Long, Iterator[I])] =>
//    //        if (gi.hasNext) {
//    //          val (index, it) = gi.next()
//    //          Some((it, gi))
//    //        } else None
//    //    }
//
//    candleIterator = groupIterator
//      .scanLeft[Option[(Long, C)]](None) {
//        // First group
//        case (None, (nextCandleIndex, nextItems)) =>
//          val initialCandle: C = scanner.empty(timeStep.toMicros * nextCandleIndex, timeStep, None)
//          val inner = nextItems.scanLeft[C](initialCandle)(updatingFold(scanner.fold))
//          Some((nextCandleIndex, inner))
//
//        // Next group. i.e. nextIndex = prevIndex + 1
//        case (Some((prevIndex, prevCandles)), (nextIndex, nextItems)) if prevIndex + 1 == nextIndex =>
//          val initialCandle: C = scanner.empty(timeStep.toMicros * nextIndex, timeStep, Some(prevCandles.last))
//          prevCandles ++ nextItems
//          Some((nextIndex, Seq(nextItems.foldLeft[C](initialCandle)(updatingFold(scanner.fold)))))
//
//        // Detected missing time steps. Fill them in.
//        case (Some((prevIndex, prevCandles)), (nextIndex, nextItems)) if prevIndex + 1 < nextIndex =>
//          val nextRealIndexMicros = timeStep.toMicros * nextIndex
//          val missingIndexCount = nextIndex - (prevIndex + 1)
//          var lastCandle = prevCandles.last
//          val empties = (1L to missingIndexCount).reverse.map { i =>
//            lastCandle = scanner.empty(nextRealIndexMicros - i * timeStep.toMicros, timeStep, Some(lastCandle))
//            lastCandle
//          }
//          val initialCandle: C = scanner.empty(nextRealIndexMicros, timeStep, Some(empties.last))
//          Some((nextIndex, empties :+ nextItems.foldLeft[C](initialCandle)(updatingFold(scanner.fold))))
//
//      }.drop(1).flatMap(_.get._2.iterator)

//    CollectionType.fromIterable(candleIterator.toIterable)
  }


  def unfoldIterator[A, S](init: S)(f: S => Option[(A, S)]): Iterator[A] = {
    var currentState = init
    var nextResultAndState: Option[(A, S)] = null

    new AbstractIterator[A] {
      override def hasNext: Boolean = {
        if (nextResultAndState == null) {
          nextResultAndState = f(currentState)
        }
        nextResultAndState.isDefined
      }

      override def next(): A = {
        assert(hasNext)
        val (result, state) = nextResultAndState.get
        currentState = state
        nextResultAndState = null
        result
      }
    }
  }


}
