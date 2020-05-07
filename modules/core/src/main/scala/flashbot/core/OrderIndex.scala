package flashbot.core

import scala.util.Try

class OrderIndex {
  val byTag = new java.util.HashMap[String, java.util.HashMap[String, OrderRef]]
  val byKey = new java.util.HashMap[String, OrderRef]
  val byClientId = new java.util.HashMap[String, OrderRef]
  var counter: Long = 0

  protected[flashbot] def insert(order: OrderRef): Unit = {
    order.seqNr = counter
    counter += 1
    byKey.put(order.key, order)
    byClientId.put(order.id, order)
    byTag.computeIfAbsent(order.tag, (key: String) => {
      new java.util.HashMap[String,OrderRef](){
        put(key, order)
      }
    })
  }

  protected[flashbot] def remove(order: OrderRef): Unit = {
    Try(byKey.remove(order.key))
    Try(byTag.remove(order.tag))
    Try(byClientId.remove(order.id))
  }
}

