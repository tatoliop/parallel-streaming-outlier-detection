package outlier

import scala.collection.mutable.ListBuffer

class StormData(data1d: Data1d) extends Data1d(data1d.value, data1d.arrival, data1d.flag, data1d.id) {
  var count_after: Int = 1
  var nn_before = ListBuffer[Long]()

  override def equals(other: Any): Boolean = other match {
    case that: Data1d => {
      value == that.value &&
        id == that.id
    }
    case that2: StormData => {
      value == that2.value &&
        id == that2.id
    }
    case _ => false
  }

  override def toString = s"StormData($id, $arrival)"
}
