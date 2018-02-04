package outlier

import mtree.DistanceFunctions.EuclideanCoordinate

import scala.collection.mutable.ListBuffer

class StormData(xc: Double, time: Long, cflag: Int, cid: Int) extends EuclideanCoordinate with Comparable[StormData] with Ordered[StormData] {
  val value: Double = xc
  val arrival: Long = time
  val state = Seq(value)
  val hashcode = state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  val flag: Int = cflag
  val id: Int = cid

  var count_after: Int = 0
  var nn_before = ListBuffer[Long]()
  var safe_inlier: Boolean = false
  var mc: Int = -1
  var rmc = ListBuffer[Int]()


  def insert_nn_before(el: Long, k: Int): Unit = {
    if (nn_before.size == k) {
      val tmp = nn_before.min
      if (el > tmp) {
        nn_before.-=(tmp)
        nn_before.+=(el)
      }
    } else {
      nn_before.+=(el)
    }
  }

  def clear(newMc: Int):Unit = {
    nn_before.clear()
    count_after = 0
    rmc.clear()
    mc = newMc
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[StormData]

  override def equals(other: Any): Boolean = other match {
    case that: StormData =>
      value == that.value &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = {
    hashcode
  }

  /**
    * The number of dimensions.
    */
  override def dimensions() = 1

  /**
    * A method to access the {@code index}-th component of the coordinate.
    *
    * @param index The index of the component. Must be less than { @link
    * #dimensions()}.
    */
  override def get(index: Int) = value

  override def compareTo(t: StormData) = {
    if(this.value > t.value) +1
    else if(this.value < t.value) -1
    else 0
  }

  override def compare(that: StormData) = this.value.compareTo(that.value)

  override def toString = s"StormData($id, $count_after, ${nn_before.size}, $safe_inlier, $mc, $rmc $flag)"
}
