package flashbot.core

import debox.Buffer
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongComparators, LongHeapPriorityQueue}
import spire.syntax.cfor._
import spire.algebra.Order

class EventBuffer(initialCapacity: Int) {
  lazy val buffer: Buffer[Tick] = debox.Buffer.fill[Tick](initialCapacity)(null)
  var size: Int = 0

  def +=(event: Tick): Unit = {
    if (size == buffer.length) {
      buffer += event
    } else if (size < buffer.length) {
      buffer(size) = event
    } else {
      throw new RuntimeException("EventBuffer size should never be larger than underlying.")
    }
    size += 1
  }

  def consume(fn: Tick => Unit): Unit = {
    cfor(0)(_ < size, _ + 1) { i =>
      fn(buffer(i))
      buffer(i) = null
    }
    size = 0
  }

}

/**
  * Simulates the event tick behavior of a trading session in live/paper mode.
  */
class EventLoop {

  // The current time from the perspective of the event loop.
  var currentMicros: Long = -1

  // Buffer management
  private var bufferPool: List[EventBuffer] = List.fill(100)(new EventBuffer(100))
  private val eventQueues: Long2ObjectOpenHashMap[EventBuffer] = new Long2ObjectOpenHashMap[EventBuffer]()
  private val heap: LongHeapPriorityQueue = new LongHeapPriorityQueue(LongComparators.NATURAL_COMPARATOR)

  private var collector = new EventBuffer(1000)
  private var collectorRegister = new EventBuffer(1000)
  private var eventStream: EventBuffer = _

  // Top level function to run the event queue until a given time.
  def run(untilMicros: Long, fn: Tick => Unit): Unit = {
    assert(untilMicros >= currentMicros, s"$untilMicros >= $currentMicros")
    assert(eventStream == null)

    // Load and run the eventStream until the last loaded stream is null.
    eventStream = loadEventStream(untilMicros)
    while (eventStream != null) {
      consumeEventStream(fn)
      eventStream = loadEventStream(untilMicros)
    }
    currentMicros = untilMicros
  }

  // Prepare the next buffer for evaluation as the event stream.
  private def loadEventStream(untilMicros: Long): EventBuffer = {
    if (collector.size > 0) {
      val stream = collector
      collector = collectorRegister
      collectorRegister = null
      stream
    } else if (!heap.isEmpty && heap.firstLong() <= untilMicros) {
      currentMicros = heap.dequeueLong()
      eventQueues.remove(currentMicros)
    } else {
      null
    }
  }

  // Try to get from pool. If non exists, create it.
  private def acquireBuffer(): EventBuffer = bufferPool match {
    case buf :: rest =>
      bufferPool = rest
      buf
    case Nil =>
      new EventBuffer(100)
  }


  def delay(delayMicros: Long, tick: Tick): Unit = {
    // If is immediate, add to collector.
    if (currentMicros == -1) {
      throw new RuntimeException("EventLoop not initialized.")
    } else if (delayMicros == 0) {
      collector += tick
    } else if (delayMicros > 0) {
      val m = currentMicros + delayMicros
      val queue = if (eventQueues.containsKey(m)) eventQueues.get(m) else acquireBuffer()
      queue += tick

      if (m < heap.firstLong())
        heap.enqueue(m)

    } else {
      throw new RuntimeException("EventLoop does not accept events from the past.")
    }
  }

  // Consume and release `eventStream`. When released, EventBuffers are always placed
  // back in the pool and they are never shrunk. The exception is if the buffer is the
  // collector, in which case just restore the collector register.
  private def consumeEventStream(fn: Tick => Unit): Unit = {
    eventStream.consume(fn)

    // Release
    if (collectorRegister == null) {
      collectorRegister = eventStream
    } else {
      bufferPool = eventStream :: bufferPool
    }
    eventStream = null
  }

}

