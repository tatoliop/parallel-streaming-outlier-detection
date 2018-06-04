package outlier

import mtree.DistanceFunctions.EuclideanCoordinate
import scala.collection.mutable.ListBuffer

class Data(xc: ListBuffer[Double], time: Long, cflag: Int, cid: Int) extends EuclideanCoordinate with Comparable[Data] with Ordered[Data] {

  val value: ListBuffer[Double] = xc
  val arrival: Long = time
  val state = Seq(value)
  val hashcode = state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  val flag: Int = cflag
  val id: Int = cid

  var count_after: Int = 0
  var nn_before = ListBuffer[Long]()
  var safe_inlier: Boolean = false
  var mc: Int = -1


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
    mc = newMc
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[Data]

  override def equals(other: Any): Boolean = other match {
    case that: Data =>
        this.value.size == that.value.size &&
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
  override def dimensions() = value.size

  /**
    * A method to access the {@code index}-th component of the coordinate.
    *
    * @param index The index of the component. Must be less than { @link
    *              #dimensions()}.
    */
  override def get(index: Int) = value(index)

  override def compareTo(t: Data) = {
    val dim = Math.min(this.dimensions(), t.dimensions())
    for (i <- 0 until dim) {
      if (this.value(i) > t.value(i)) +1
      else if (this.value(i) < t.value(i)) -1
      else 0
    }
    if (this.dimensions() > dim) +1
    else -1
  }

  override def compare(that: Data) = this.compareTo(that)

  override def toString = s"StormData($id, $value, $flag, $count_after, ${nn_before.size})"
}
