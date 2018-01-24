package outlier

import scala.collection.mutable.ListBuffer

class Node(idc: Long, cid: Int, x: Int, neighbors: Int, flagc: Int) extends Ordered[Node] {
  //constructor for isb node
  private val point = new Point(x) //node object
  private val id = idc //time id
  private val count_id = cid
  //counter id
  private var count_after: Int = 1 //neighbors after this node
  private var nn_before = ListBuffer[Long]() //neighbors before this node
  private var safe_inlier: Boolean = false //if safe then it is an inlier for the rest of its life
  private val k = neighbors //k-nn for checking inliers
  private val flag = flagc //range for distance queries
  private var windowStart: Long = 0
  private var windowEnd: Long = 0
  private var customKey = ""

  //getters
  def get_id(): Long = {
    id
  }

  def get_flag(): Int = {
    flag
  }

  def get_cid(): Int = {
    count_id
  }

  def get_point(): Point = {
    point
  }

  def get_safe(): Boolean = {
    safe_inlier
  }

  def get_nn_before(n: Long): Int = {
    val tmpList = nn_before.filter(_ >= n)
    tmpList.size
  }

  def get_count_after(): Int = {
    count_after
  }

  def get_window_start(): Long = {
    windowStart
  }

  def get_window_end(): Long = {
    windowEnd
  }

  def get_custom_key(): String = {
    customKey
  }

  //setters
  def set_window(t1: Long, t2: Long) {
    windowStart = t1
    windowEnd = t2
    customKey = windowStart + "-" + windowEnd + "-" + count_id
  }

  def set_count_after(up: Int = 1) {
    count_after += up
    if (count_after >= k) { //if k or more neighbors then object is safe
      safe_inlier = true
    }
  }

  //add previous neighbors
  def add_nn_before(n: Long) {
    if (nn_before.size == k) { //max size must be k
      val temp_nn = nn_before.min
      nn_before -= temp_nn
    }
    nn_before += n
  }

  //distance between nodes is distance between points
  def distance(node: Node): Int = {
    val temp_p = node.get_point()
    val res = point.distance(temp_p)
    return res
  }


  //override def toString = s"Node($point, $id, $count_id, $nn_before, $count_after, $safe_inlier)"
  override def toString = s"Node($point)"
  //override def toString = s"Node($count_id, $flag, $id)"
  //override def toString = s"Node($customKey, $flag)"

  def canEqual(other: Any): Boolean = other.isInstanceOf[Node]

  override def equals(other: Any): Boolean = other match {
    case that: Node =>
      (that canEqual this) &&
        id == that.id &&
        count_id == that.count_id
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, count_id)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def compare(that: Node) = {
    this.get_cid().compareTo(that.get_cid())
  }
}
