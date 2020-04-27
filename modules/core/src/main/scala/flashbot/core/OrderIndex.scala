package flashbot.core

import scala.util.Try

class OrderIndex {
  val byTag = new java.util.HashMap[String, java.util.HashMap[String, OrderRef]]
  val byKey = new java.util.HashMap[String, OrderRef]
  val byClientId = new java.util.HashMap[String, OrderRef]  // TODO: check where clientId comes from ?
  var counter: Long = 0

  protected[flashbot] def insert(order: OrderRef): Unit = {
    order.seqNr = counter
    counter += 1
    byKey.put(order.key, order)
    byTag.computeIfAbsent(order.tag, (key: String) => {
      new java.util.HashMap[String,OrderRef](){
        put(key, order)
      }
    })
    // TODO: whre to get clientId ?
    //byClientId.put(order)
  }

  protected[flashbot] def remove(order: OrderRef): Unit = {
    Try(byKey.remove(order.key))
    Try(byTag.remove(order.tag))

  }
}

