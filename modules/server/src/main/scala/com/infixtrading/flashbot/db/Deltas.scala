package com.infixtrading.flashbot.db

import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._

class Deltas(tag: Tag) extends Table[(Long, Long, Long, String)](tag, "flashbot_deltas") {
  def bundle = column[Long]("bundle")
  def seqid = column[Long]("seqid")
  def micros = column[Long]("micros")
  def data = column[String]("data")
  override def * = (bundle, seqid, micros, data)
}
