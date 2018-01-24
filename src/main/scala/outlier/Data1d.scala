package outlier

import mtree.DistanceFunctions.EuclideanCoordinate

class Data1d(xc: Double, time: Long, cflag: Int, cid: Int) extends EuclideanCoordinate with Comparable[Data1d] with Ordered[Data1d] {
  val value: Double = xc
  val arrival: Long = time
  val state = Seq(value)
  val hashcode = state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  val flag: Int = cflag
  val id: Int = cid

  def canEqual(other: Any): Boolean = other.isInstanceOf[Data1d]

  override def equals(other: Any): Boolean = other match {
    case that: Data1d =>
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

  override def compareTo(t: Data1d) = {
    if(this.value > t.value) +1
    else if(this.value < t.value) -1
    else 0
  }

  override def compare(that: Data1d) = this.value.compareTo(that.value)


  override def toString = s"Data1d($value, $arrival, $flag, $id)"
}
