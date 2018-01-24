package outlier

//class for stream object (1d point)
class Point(xc: Int) {
  //constructor for point
  private val x: Int = xc

  //getters
  def get_x() : Int = {
    return x
  }
  //get distance between 2 points
  def distance(p2 : Point) : Int = {
    val res = scala.math.abs(x - p2.get_x())
    return res
  }
  override def toString = s"Point($x)"
}


