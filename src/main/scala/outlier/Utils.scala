package outlier

object Utils {

  def distance(xs: Data, ys: Data): Double = {
    val min = Math.min(xs.dimensions(), ys.dimensions())
    var value: Double = 0
    for (i <- 0 until min) {
      value += scala.math.pow(xs.value(i) - ys.value(i), 2)
    }
    val res = scala.math.sqrt(value)

    res
  }

  def distance(xs: Data, ys: MicroCluster): Double = {
    val min = Math.min(xs.dimensions(), ys.center.size)
    var value: Double = 0
    for (i <- 0 until min) {
      value += scala.math.pow(xs.value(i) - ys.center(i), 2)
    }
    val res = scala.math.sqrt(value)
    res
  }

  def combineNewElementsAdvanced(el1: Data, el2: Data, k: Int): Data = {
    if (el1 == null) {
      el2
    } else if (el2 == null) {
      el1
    } else if (el1.flag == 1 && el2.flag == 1) {
      for (elem <- el2.nn_before) {
        el1.insert_nn_before(elem, k)
      }
      el1
    } else if (el2.flag == 0) {
      for (elem <- el1.nn_before) {
        el2.insert_nn_before(elem, k)
      }
      el2
    } else if (el1.flag == 0) {
      for (elem <- el2.nn_before) {
        el1.insert_nn_before(elem, k)
      }
      el1
    } else {
      null
    }
  }

  def combineOldElementsAdvanced(el1: Data, el2: Data, k: Int): Data = {
    el1.count_after = el2.count_after
    el1.safe_inlier = el2.safe_inlier
    el1
  }

  def combineElementsParallel(el1: Data, el2: Data, k: Int): Data = {
    for (elem <- el2.nn_before) {
      el1.insert_nn_before(elem, k)
    }
    el1
  }
}
