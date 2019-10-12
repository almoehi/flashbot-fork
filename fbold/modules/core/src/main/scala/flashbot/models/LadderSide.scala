package flashbot.models

import java.lang

import flashbot.core.{Ask, Bid, Matching, QuoteSide}
import flashbot.util.NumberUtils
import it.unimi.dsi.fastutil.doubles.DoubleArrayFIFOQueue

import scala.util.Random

/**
  * A ring buffer of the sizes at the top N levels of a side of a ladder.
  * FIFO is misleading here because it supports deque (double ended queue) ops.
  * Entries in the backing array may be 0. Accessor methods should hide this fact.
  * Additionally, this class is about as thread un-safe as you can get.
  *
  * @param maxDepth the number of price levels (not including empty levels) to support.
  * @param tickSize the price difference between consecutive levels.
  * @param side if this is a bid or ask ladder.
  */
class LadderSide(val maxDepth: Int,
                 val tickSize: Double,
                 val side: QuoteSide)
    extends DoubleArrayFIFOQueue(maxDepth * 2) with Matching {

  assert(maxDepth > 0, "Max ladder depth must be > 0")

  // The number of non-empty levels in the ladder. This will be equal to `size` if
  // there are no empty levels between the best and worst.
  var depth: Int = 0
  var totalQty: Double = 0

  var tickScale: Int = NumberUtils.scale(tickSize)

  var bestPrice: Double = java.lang.Double.NaN
  var worstPrice: Double = java.lang.Double.NaN

  def bestQty: Double = array(firstIndex)
  def worstQty: Double = array(lastIndex)
  def round(price: Double): Double = NumberUtils.round(price, tickScale)

  private val random = new Random()

  def copy(): LadderSide = {
    val newLadder = new LadderSide(maxDepth, tickSize, side)
    copyInto(newLadder)
    newLadder
  }

  def copyInto(dest: LadderSide): Unit = {
    assert(dest.maxDepth == maxDepth && dest.tickSize == tickSize && dest.side == side)

    // Copy backing array
    if (dest.length != length) {
      dest.array = Array.ofDim[Double](length)
      dest.length = length
    }
    System.arraycopy(array, 0, dest.array, 0, length)

    // Copy vars
    if (dest.start != start) dest.start = start
    if (dest.end != end) dest.end = end
    if (dest.depth != depth) dest.depth = depth
    if (dest.totalQty != totalQty) dest.totalQty = totalQty
    if (dest.bestPrice != bestPrice) dest.bestPrice = bestPrice
    if (dest.worstPrice != worstPrice) dest.worstPrice = worstPrice
  }

  /**
    * @param price the price level to update.
    * @param qty the qty at that price, or 0 to remove the price level.
    */
  def update(price: Double, qty: Double): Unit = {

    val level = levelOfPrice(price)
    if (qty == 0) {
      try {
        assert(depth > 0, "Cannot remove level from empty ladder")
      } catch {
        case e: AssertionError =>
          throw e
      }
      val qtyToRemove = qtyAtLevel(level)

//      assert(qtyToRemove > 0, s"Cannot remove empty level: $level")
      if (qtyToRemove > 0) {
        depth -= 1
        totalQty = NumberUtils.round8(totalQty - qtyToRemove)
      }
      array(indexOfLevel(level)) = 0
      trimLevels()

    } else if (qty > 0) {
      // Base case. If depth is at zero, simply enqueue the qty.
      if (depth == 0) {
        enqueue(qty)
        bestPrice = price
        worstPrice = price
        depth = 1
        totalQty = qty

      // If level < 0, then we need to prepend that many empty levels and set the
      // qty of the first level to the given qty.
      } else if (level < 0) {
        padLeft(-level)
        array(firstIndex) = qty
        depth += 1
        totalQty = NumberUtils.round8(totalQty + qty)
        truncate()

      // If level >= size, then we need to append that many levels and set the qty
      // of the last level to the given qty.
      } else if (level >= size) {
        assert(depth < maxDepth, s"Can't add the level ($price, $qty) because the ladder is full.")
        padRight(level - size + 1)
        array(lastIndex) = qty
        depth += 1
        totalQty = NumberUtils.round8(totalQty + qty)

      // Otherwise, we are updating an existing, possibly empty, level.
      } else {
        val existingQty = qtyAtLevel(level)
        if (existingQty == 0) {
          depth += 1
        }
        totalQty = NumberUtils.round8(totalQty + (qty - existingQty))
        array(indexOfLevel(level)) = qty
        truncate()
      }
    } else throw new RuntimeException("Quantity cannot be set to a negative number")
  }

  // Returns a price level from this ladder side at random where levels with higher qtys
  // have a higher chance of being selected.
  def randomPriceLevelWeighted: Double = {
    if (isEmpty) throw new IllegalStateException("Cannot select random price from empty ladder side")
    val limit = random.nextDouble() * totalQty;
    var sum = 0d
    var price = bestPrice
    val it = iterator()
    while (it.hasNext && sum < limit) {
      price = it.next()._1
      sum += qtyAtPrice(price)
    }
    price
  }

  private def firstIndex: Int = start
  private def lastIndex: Int = (if (end == 0) length else end) - 1

  // Removes elements from tail while depth exceeds the max depth.
  private def truncate(): Unit = {
    trimLevels()
    while (depth > maxDepth) {
      val removedQty = dequeueLastDouble()
      worstPrice = round(side.makeBetterBy(worstPrice, tickSize))

      if (removedQty > 0) {
        depth -= 1
      }

      trimLevels()
    }
  }

  // Ensures that `start` and `end` always point to non-empty levels.
  // Also updates the best and worst prices.
  private def trimLevels(): Unit = {
    var removedFromBest: Int = 0
    var removedFromWorst: Int = 0

    while (nonEmpty && firstDouble() == 0) {
      dequeueDouble()
      removedFromBest = removedFromBest + 1
    }

    while (nonEmpty && lastDouble() == 0) {
      dequeueLastDouble()
      removedFromWorst = removedFromWorst + 1
    }

    if (isEmpty) {
      bestPrice = java.lang.Double.NaN
      worstPrice = java.lang.Double.NaN
    } else {
      bestPrice = round(side.makeWorseBy(bestPrice, tickSize * removedFromBest))
      worstPrice = round(side.makeBetterBy(worstPrice, tickSize * removedFromWorst))
    }
  }

  private def padLeft(n: Int): Unit = {
    var i = 0
    while (i < n) {
      enqueueFirst(0)
      bestPrice = round(side.makeBetterBy(bestPrice, tickSize))
      i += 1
    }
  }

  private def padRight(n: Int): Unit = {
    var i = 0
    while (i < n) {
      enqueue(0)
      worstPrice = round(side.makeWorseBy(worstPrice, tickSize))
      i += 1
    }
  }

  def indexOfLevel(level: Int): Int = (start + level + math.abs(level * length)) % length

  def qtyAtLevel(level: Int): Double = {
    try {
      array(indexOfLevel(level))
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        e.printStackTrace()
        throw e
    }
  }

  def qtyAtPrice(price: Double): Double = qtyAtLevel(levelOfPrice(price))

  def levelOfPrice(price: Double): Int = {
    if (isEmpty) {
      return 0;
    }

    math.round(side.isWorseBy(price, bestPrice) / tickSize).toInt
  }

  def priceOfLevel(level: Int): Double =
    round(side.makeWorseBy(bestPrice, level * tickSize))

  def nonEmpty: Boolean = !isEmpty

  override def matchMutable(quoteSide: QuoteSide,
                            approxPriceLimit: Double,
                            approxSize: Double): Double = {
    matchCount = 0
    matchTotalQty = 0d

    val size = NumberUtils.round8(approxSize)
    val priceLimit = round(approxPriceLimit)
    var remainder = size
    while (this.nonEmpty && remainder > 0 && side.isBetterOrEq(bestPrice, priceLimit)) {
      val matchQty = math.min(remainder, bestQty)
      remainder = NumberUtils.round8(remainder - matchQty)
      matchPrices(matchCount) = bestPrice
      matchQtys(matchCount) = matchQty
      matchCount += 1
      matchTotalQty = matchQty + matchTotalQty
      update(bestPrice, NumberUtils.round8(bestQty - matchQty))
    }
    matchPrices(matchCount) = -1
    matchQtys(matchCount) = -1
    matchTotalQty = NumberUtils.round8(matchTotalQty)

    remainder
  }


  override def matchSilent(quoteSide: QuoteSide,
                           approxPriceLimit: Double,
                           approxSize: Double): Double = {
    matchCount = 0
    matchTotalQty = 0d

    val size = NumberUtils.round8(approxSize)
    val priceLimit = round(approxPriceLimit)
    var remainder = size
    var i = 0
    var price = bestPrice
    while (remainder > 0 && side.isBetterOrEq(price, priceLimit)) {
      val qty = qtyAtPrice(price)
      val matchQty = math.min(remainder, qty)
      remainder = NumberUtils.round8(remainder - matchQty)
      matchPrices(i) = price
      matchQtys(i) = matchQty
      matchCount += 1
      matchTotalQty = matchQty + matchTotalQty
      i += 1
      price = nextPrice(price)
    }
    matchPrices(i) = -1
    matchQtys(i) = -1
    matchTotalQty = NumberUtils.round8(matchTotalQty)
    remainder
  }

  override def matchSilentAvg(quoteSide: QuoteSide,
                              approxPriceLimit: Double,
                              approxSize: Double): (Double, Double) = {
    matchCount = 0
    matchTotalQty = 0d

    val size = NumberUtils.round8(approxSize)
    val priceLimit = round(approxPriceLimit)
    var unroundedAvgPrice: Double = java.lang.Double.NaN
    var break = false
    var price = bestPrice
    while (!break && side.isBetterOrEq(price, priceLimit)) {
      val remainder = NumberUtils.round8(size - matchTotalQty)
      if (remainder > 0) {
        val qty = qtyAtPrice(price)
        val matchQty = math.min(remainder, qty)
        unroundedAvgPrice =
          if (java.lang.Double.isNaN(unroundedAvgPrice)) price
          else (unroundedAvgPrice * matchTotalQty + price * matchQty) / (matchTotalQty + matchQty)
        matchCount += 1
        matchTotalQty = NumberUtils.round8(matchTotalQty + matchQty)
        price = nextPrice(price)
      } else break = true
    }
    (matchTotalQty, unroundedAvgPrice)
  }

  def hasNextPrice(curPrice: Double): Boolean =
    if (java.lang.Double.isNaN(curPrice)) nonEmpty
    else side.isBetter(curPrice, worstPrice)

  def nextPrice(curPrice: Double): Double = {
    if (!hasNextPrice(curPrice)) return java.lang.Double.NaN
    if (java.lang.Double.isNaN(curPrice)) return bestPrice

    var l = levelOfPrice(curPrice) + 1
    var p = priceOfLevel(l)
    while (qtyAtPrice(p) == 0) {
      l += 1
      p = priceOfLevel(l)
    }
    p
  }

  def hasPrevPrice(curPrice: Double): Boolean =
    if (java.lang.Double.isNaN(curPrice)) nonEmpty
    else curPrice > bestPrice

  def prevPrice(curPrice: Double): Double = {
    if (!hasPrevPrice(curPrice)) return java.lang.Double.NaN
    if (java.lang.Double.isNaN(curPrice)) return worstPrice

    var l = levelOfPrice(curPrice) + 1
    var p = priceOfLevel(l)
    while (qtyAtPrice(p) == 0) {
      l -= 1
      p = priceOfLevel(l)
    }
    p
  }

  def iterator(): Iterator[(Double, Double)] = new LadderSidePriceIterator().map(p => (p, qtyAtPrice(p)))

  class LadderSidePriceIterator(initialPrice: Double = java.lang.Double.NaN) extends Iterator[Double] {

    private var p = initialPrice

    override def hasNext: Boolean = hasNextPrice(p)

    override def next(): Double = {
      val n = nextPrice(p)
      p = n
      n
    }
  }

}