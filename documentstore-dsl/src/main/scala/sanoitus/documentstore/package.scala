package sanoitus

package object documentstore {

  def mapDiff[A](from: Map[String, A], to: Map[String, A]): Map[String, Option[A]] = {
    val changes = from.foldLeft(Map[String, Option[A]]()) { (acc, a) =>
      val (key, value) = a
      to.get(key) match {
        case None                  => acc + ((key, None))
        case Some(v) if value != v => acc + ((key, Some(v)))
        case _                     => acc
      }
    }

    (to.keySet -- from.keySet).foldLeft(changes) { (acc, a) => acc + ((a, to.get(a))) }
  }
}
