package common_utils

import mtree.DistanceFunctions.EuclideanCoordinate
import common_utils.NodeType.NodeType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//EventQ Var
object NodeType extends Enumeration with Serializable {
  type NodeType = Value
  val OUTLIER, INLIER_MC, INLIER_PD, SAFE_INLIER = Value
}
//END EventQ Var

class Data(xc: ListBuffer[Double], time: Long, cflag: Int, cid: Int) extends EuclideanCoordinate with Comparable[Data] with Ordered[Data] with Serializable {

  val value: ListBuffer[Double] = xc
  val arrival: Long = time
  val state = Seq(value)
  val hashcode = state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  val flag: Int = cflag
  val id: Int = cid

  var count_after: Int = 0
  var nn_before = ListBuffer[Long]()
  var safe_inlier: Boolean = false

  //MCOD Vars
  var mc: Int = -1
  var Rmc = mutable.HashSet[Int]()
  //END MCOD Vars

  //EventQ Var
  var node_type: NodeType = null
  //END EventQ Var

  //Slicing Vars
  var slices_before = mutable.HashMap[Long, Int]()
  var last_check: Long = 0L
  //END Slicing Vars


  //pAMCOD Stuff
  var nn_before_set = ListBuffer[(Long,Double)]()
  var count_after_set = ListBuffer[Double]()
  //END pAMCOD Stuff

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

  def get_min_nn_before(time: Long): Long = {
    if(nn_before.count(_ >= time) == 0) 0L
    else nn_before.filter(_ >= time).min
  }

  //MCOD & AMCOD & KSKY Function
  def clear(newMc: Int):Unit = {
    nn_before_set.clear() //AMCOD
    count_after_set.clear() //AMCOD
    nn_before.clear()
    lSky.clear() //KSKY
    count_after = 0
    mc = newMc
  }
  //END MCOD & AMCOD & KSKY Function

  //KSKY & PSOD stuff
  var lSky = mutable.HashMap[Int, ListBuffer[(Int,Long)]]()
  //END KSKY & PSOD stuff

  def canEqual(other: Any): Boolean = other.isInstanceOf[Data]

  override def equals(other: Any): Boolean = other match {
    case that: Data =>
        this.value.size == that.value.size &&
        this.value == that.value &&
        this.id == that.id
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

  //override def toString = s"Data($value, $id, $flag, $count_after, ${nn_before.size}, $safe_inlier, $mc)"
  override def toString = s"($id,$arrival)"
}
