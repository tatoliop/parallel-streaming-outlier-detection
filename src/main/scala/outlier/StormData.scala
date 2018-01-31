package outlier

import scala.collection.mutable.ListBuffer

class StormData(data1d: Data1d) extends Data1d(data1d.value, data1d.arrival, data1d.flag, data1d.id) {
  var count_after: Int = 1
  var nn_before = ListBuffer[Long]()
  var safe_inlier: Boolean = false

  def insert_nn_before(el: Long, k: Int): Unit ={
    if(nn_before.size == k){
      val tmp = nn_before.min
      nn_before.-=(tmp)
      nn_before.+=(el)
    }else{
      nn_before.+=(el)
    }
  }

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
