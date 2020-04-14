package flashbot.db

import slick.lifted.Tag
import slick.jdbc.PostgresProfile.api._

class Snapshots(tag: Tag) extends Table[SnapshotRow](tag, "flashbot_snapshots") {
  def id = column[Long]("id", O.AutoInc)
  def bundle = column[Long]("bundle")
  def seqid = column[Long]("seqid")
  def micros = column[Long]("micros")
  def data = column[String]("data")
  def backfill = column[Option[Long]]("backfill")
  override def * = (id, bundle, seqid, micros, data, backfill) <> (SnapshotRow.tupled, SnapshotRow.unapply)
  def idx_snaps_id = index("idx_snaps_id", id)
  def idx_snaps = index("idx_snaps", (bundle, seqid), unique = true)
  def idx_snaps_micros = index("idx_snaps_micros", micros)
}