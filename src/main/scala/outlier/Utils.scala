package outlier

import scala.collection.mutable.ListBuffer

object Utils {

  //hardcoded spatial partitioning
  val spatial_tao = Map[Int, String](1 -> "-0.01", 2 -> "79.23!82.47!85.77", 3 -> "26.932")
  val spatial_fc_5d = Map[Int, String](1 -> "0.56878", 2 -> "0.35278",
    3 -> "0.19697", 4 -> "0.15605", 5 -> "0.26227")
  val spatial_fc_2d = Map[Int, String](1 -> "58.0!127.0!260.0", 2 -> "7.0!30.0!69.0")
  val spatial_stock = Map[Int, String](
    1 -> "87.231!94.222!96.5!97.633!98.5!99.25!99.897!100.37!101.16!102.13!103.18!104.25!105.25!106.65!109.75")
  val spatial_stock_parallelism = Map[Int, String](2 -> "100.37",
    4 -> "97.633!100.37!104.25",
    8 -> "94.222!97.633!99.25!100.37!102.13!104.25!106.65",
    12 -> "90.7!95.965!97.633!98.75!99.7!100.37!101.49!102.84!104.25!105.59!108.36",
    32 -> "77.457!87.231!91.88!94.222!95.59!96.5!97.125!97.633!98.074!98.5!98.888!99.25!99.588!99.897!100.07!100.37!100.72!101.16!101.65!102.13!102.65!103.18!103.72!104.25!104.78!105.25!105.79!106.65!107.84!109.75!112.14",
    16 -> "87.231!94.222!96.5!97.633!98.5!99.25!99.897!100.37!101.16!102.13!103.18!104.25!105.25!106.65!109.75")


  def replicationPartitioning(parallelism: Int,
                              value: ListBuffer[Double],
                              time: Long, id: Int): ListBuffer[(Int, Data)] = {
    var list = new ListBuffer[(Int, Data)]
    for (i <- 0 until parallelism) {
      var flag = 0
      if (id % parallelism == i) flag = 0
      else flag = 1
      val tmpEl = (i, new Data(value, time, flag, id))
      list.+=(tmpEl)
    }
    list
  }

  def gridPartitioning(parallelism: Int,
                       value: ListBuffer[Double],
                       time: Long,
                       id: Int,
                       range: Double,
                       dataset: String): ListBuffer[(Int, Data)] = {

    val res: (Int, ListBuffer[Int]) = dataset match {
      case "stock" => findPartSTOCK(value, range)
      case "fc" => findPartFC_2d(value, range)
      case "tao" => findPartTAO(value, range)
      case _ => null
    }

    val partitionBelong = res._1
    val neighbors = res._2
    var list = new ListBuffer[(Int, Data)]
    val tmpEl = (partitionBelong, new Data(value, time, 0, id))
    list.+=(tmpEl)
    if (neighbors.nonEmpty) {
      neighbors.foreach(p => {
        val tmpEl2 = (p, new Data(value, time, 1, id))
        list.+=(tmpEl2)
      })
    }
    list
  }

  def findPartTAO(value: ListBuffer[Double], range: Double): (Int, ListBuffer[Int]) = {
    val points1d = spatial_tao(1).split("!").map(_.toDouble).toList
    val points2d = spatial_tao(2).split("!").map(_.toDouble).toList
    val points3d = spatial_tao(3).split("!").map(_.toDouble).toList

    var belongs_to = ListBuffer[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    var neighbors = ListBuffer[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    //1st dimension=============================
    if (value(0) <= points1d(0)) { //it belongs to x1 (1,2,3,4,5,6,7,8)
      belongs_to.-=(9, 10, 11, 12, 13, 14, 15, 16)
      if (value(0) >= points1d(0) - range) { //belongs to x2 too
        //nothing to do
      } else { //does not belong to x2
        neighbors.-=(9, 10, 11, 12, 13, 14, 15, 16)
      }
    } else { //it belongs to x2 (9,10,11,12,13,14,15,16)
      belongs_to.-=(1, 2, 3, 4, 5, 6, 7, 8)
      if (value(0) <= points1d(0) + range) { //belongs to x1 too
        //nothing to do
      } else {
        //does not belong to x1
        neighbors.-=(1, 2, 3, 4, 5, 6, 7, 8)
      }
    }
    //2nd dimension=============================
    if (value(1) <= points2d(0)) { //it belongs to y1 (1,5,9,13)
      belongs_to.-=(2, 6, 10, 14, 3, 7, 11, 15, 4, 8, 12, 16)
      neighbors.-=(3, 7, 11, 15, 4, 8, 12, 16) //y3 and y4 are not neighbors
      if (value(1) >= points2d(0) - range) { //belongs to y2 too
        //nothing to do
      } else {
        neighbors.-=(2, 6, 10, 14)
      }
    } else if (value(1) <= points2d(1)) { //it belongs to y2 (2,6,10,14)
      belongs_to.-=(1, 5, 9, 13, 3, 7, 11, 15, 4, 8, 12, 16)
      neighbors.-=(4, 8, 12, 16) //y4 is not neighbor
      if (value(1) <= points2d(0) + range) { //belongs to y1 too
        neighbors.-=(3, 7, 11, 15) //y3 is not neighbor
      } else if (value(1) >= points2d(1) - range) { //belongs to y3 too
        neighbors.-=(1, 5, 9, 13) //y1 is not neighbor
      } else {
        //y1 and y3 are not neighbors
        neighbors.-=(1, 5, 9, 13, 3, 7, 11, 15)
      }
    } else if (value(1) <= points2d(2)) { //it belongs to y3 (3,7,11,15)
      belongs_to.-=(1, 5, 9, 13, 2, 6, 10, 14, 4, 8, 12, 16)
      neighbors.-=(1, 5, 9, 13) //y1 is not neighbor
      if (value(1) <= points2d(1) + range) { //belongs to y2 too
        neighbors.-=(4, 8, 12, 16) //y4 is not neighbor
      } else if (value(1) >= points2d(2) - range) { //belongs to y4 too
        neighbors.-=(2, 6, 10, 14) //y2 is not neighbor
      } else {
        //y2 and y4 are not neighbors
        neighbors.-=(2, 6, 10, 14, 4, 8, 12, 16)
      }
    } else { //it belongs to y4 (4,8,12,16)
      belongs_to.-=(1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15)
      neighbors.-=(1, 5, 9, 13, 2, 6, 10, 14) //y1 and y2 are not neighbors
      if (value(1) <= points2d(2) + range) { //belongs to y3 too
        //nothing to do
      } else { //does not belong to y3
        neighbors.-=(3, 7, 11, 15)
      }
    }
    //3rd dimension=============================
    if (value(2) <= points3d(0)) { //it belongs to z1 (5,6,7,8,9,10,11,12)
      belongs_to.-=(1, 2, 3, 4, 13, 14, 15, 16)
      if (value(2) >= points3d(0) - range) { //belongs to z2 too
        //nothing to do
      } else { //does not belong to z2
        neighbors.-=(1, 2, 3, 4, 13, 14, 15, 16)
      }
    } else { //it belongs to z2 (1,2,3,4,13,14,15,16)
      belongs_to.-=(5, 6, 7, 8, 9, 10, 11, 12)
      if (value(2) <= points3d(0) + range) { //belongs to z1 too
        //nothing to do
      } else {
        //does not belong to z1
        neighbors.-=(5, 6, 7, 8, 9, 10, 11, 12)
      }
    }
    val partition = belongs_to.head
    neighbors.-=(partition)
    (partition, neighbors)
  }

  def findPartFC_5d(value: ListBuffer[Double], range: Double): (Int, ListBuffer[Int]) = {
    val points1d = spatial_fc_5d(1).split("!").map(_.toDouble).toList
    val points2d = spatial_fc_5d(2).split("!").map(_.toDouble).toList
    val points3d = spatial_fc_5d(3).split("!").map(_.toDouble).toList
    val points4d = spatial_fc_5d(4).split("!").map(_.toDouble).toList
    val points5d = spatial_fc_5d(5).split("!").map(_.toDouble).toList

    var belongs_to = ListBuffer[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
      23, 24, 25, 26, 27, 28, 29, 30, 31, 32)
    var neighbors = ListBuffer[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
      23, 24, 25, 26, 27, 28, 29, 30, 31, 32)
    //1st dimension=============================
    if (value(0) <= points1d(0)) { //it belongs to x1 (1,2,3,4,9,10,11,12,17,18,19,20,25,26,27,28)
      belongs_to.-=(5, 6, 7, 8, 13, 14, 15, 16, 21, 22, 23, 24, 29, 30, 31, 32)
      if (value(0) >= points1d(0) - range) { //belongs to x2 too
        //nothing to do
      } else { //does not belong to x2
        neighbors.-=(5, 6, 7, 8, 13, 14, 15, 16, 21, 22, 23, 24, 29, 30, 31, 32)
      }
    } else { //it belongs to x2 (5,6,7,8,13,14,15,16,21,22,23,24,29,30,31,32)
      belongs_to.-=(1, 2, 3, 4, 9, 10, 11, 12, 17, 18, 19, 20, 25, 26, 27, 28)
      if (value(0) <= points1d(0) + range) { //belongs to x1 too
        //nothing to do
      } else {
        //does not belong to x1
        neighbors.-=(1, 2, 3, 4, 9, 10, 11, 12, 17, 18, 19, 20, 25, 26, 27, 28)
      }
    }
    //2nd dimension=============================
    if (value(1) <= points2d(0)) { //it belongs to y1 (17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32)
      belongs_to.-=(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
      if (value(1) >= points2d(0) - range) { //belongs to y2 too
        //nothing to do
      } else { //does not belong to y2
        neighbors.-=(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
      }
    } else { //it belongs to y2 (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)
      belongs_to.-=(17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32)
      if (value(1) <= points2d(0) + range) { //belongs to y1 too
        //nothing to do
      } else {
        //does not belong to y1
        neighbors.-=(17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32)
      }
    }
    //3rd dimension=============================
    if (value(2) <= points3d(0)) { //it belongs to z1 (3,4,5,6,11,12,13,14,19,20,21,22,27,28,29,30)
      belongs_to.-=(1, 2, 9, 10, 17, 18, 25, 26, 7, 8, 15, 16, 23, 24, 31, 32)
      if (value(2) >= points3d(0) - range) { //belongs to z2 too
        //nothing to do
      } else { //does not belong to z2
        neighbors.-=(1, 2, 9, 10, 17, 18, 25, 26, 7, 8, 15, 16, 23, 24, 31, 32)
      }
    } else { //it belongs to z2 (1,2,9,10,17,18,25,26,7,8,15,16,23,24,31,32)
      belongs_to.-=(3, 4, 5, 6, 11, 12, 13, 14, 19, 20, 21, 22, 27, 28, 29, 30)
      if (value(2) <= points3d(0) + range) { //belongs to z1 too
        //nothing to do
      } else {
        //does not belong to z1
        neighbors.-=(3, 4, 5, 6, 11, 12, 13, 14, 19, 20, 21, 22, 27, 28, 29, 30)
      }
    }
    //4th dimension=============================
    if (value(3) <= points4d(0)) { //it belongs to a1 (2,3,10,11,18,19,26,27,6,7,14,15,22,23,30,31)
      belongs_to.-=(1, 4, 9, 12, 17, 20, 25, 28, 5, 8, 13, 16, 21, 24, 29, 32)
      if (value(3) >= points4d(0) - range) { //belongs to a2 too
        //nothing to do
      } else { //does not belong to a2
        neighbors.-=(1, 4, 9, 12, 17, 20, 25, 28, 5, 8, 13, 16, 21, 24, 29, 32)
      }
    } else { //it belongs to a2 (1,4,9,12,17,20,25,28,5,8,13,16,21,24,29,32)
      belongs_to.-=(2, 3, 10, 11, 18, 19, 26, 27, 6, 7, 14, 15, 22, 23, 30, 31)
      if (value(3) <= points4d(0) + range) { //belongs to a1 too
        //nothing to do
      } else {
        //does not belong to a1
        neighbors.-=(2, 3, 10, 11, 18, 19, 26, 27, 6, 7, 14, 15, 22, 23, 30, 31)
      }
    }
    //5th dimension=============================
    if (value(4) <= points5d(0)) { //it belongs to b1 (1,2,3,4,5,6,7,8,25,26,27,28,29,30,31,32)
      belongs_to.-=(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24)
      if (value(4) >= points5d(0) - range) { //belongs to b2 too
        //nothing to do
      } else { //does not belong to b2
        neighbors.-=(9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24)
      }
    } else { //it belongs to b2 (9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24)
      belongs_to.-=(1, 2, 3, 4, 5, 6, 7, 8, 25, 26, 27, 28, 29, 30, 31, 32)
      if (value(4) <= points5d(0) + range) { //belongs to b1 too
        //nothing to do
      } else {
        //does not belong to b1
        neighbors.-=(1, 2, 3, 4, 5, 6, 7, 8, 25, 26, 27, 28, 29, 30, 31, 32)
      }
    }

    val partition = belongs_to.head
    neighbors.-=(partition)

    (partition, neighbors)
  }

  def findPartSTOCK(value: ListBuffer[Double], range: Double): (Int, ListBuffer[Int]) = {

    val points = spatial_stock(1).split("!").map(_.toDouble).toList
    val parallelism = points.size + 1
    var neighbors = ListBuffer[Int]()

    var i = 0
    var break = false
    var belongs_to, previous, next = -1
    do {
      if (value(0) <= points(i)) {
        belongs_to = i //belongs to the current partition
        break = true
        if (i != 0) {
          //check if it is near the previous partition
          if (value(0) <= points(i - 1) + range) {
            previous = i - 1
          }
        } //check if it is near the next partition
        if (value(0) >= points(i) - range) {
          next = i + 1
        }
      }
      i += 1
    } while (i <= parallelism - 2 && !break)
    if (!break) {
      // it belongs to the last partition
      belongs_to = parallelism - 1
      if (value(0) <= points(parallelism - 2) + range) {
        previous = parallelism - 2
      }
    }

    val partition = belongs_to
    if (next != -1) neighbors.+=(next)
    if (previous != -1) neighbors.+=(previous)
    (partition, neighbors)
  }

  def findPartFC_2d(value: ListBuffer[Double], range: Double): (Int, ListBuffer[Int]) = {
    val points1d = spatial_fc_2d(1).split("!").map(_.toDouble).toList
    val points2d = spatial_fc_2d(2).split("!").map(_.toDouble).toList

    var belongs_to = ListBuffer[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    var neighbors = ListBuffer[Int](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
    //1st dimension=============================
    if (value(0) <= points1d(0)) { //it belongs to x1 (1,2,3,4)
      belongs_to.-=(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
      neighbors.-=(9, 10, 11, 12, 13, 14, 15, 16) //x3 and x4 are not neighbors
      if (value(0) >= points1d(0) - range) { //belongs to x2 too
        //nothing to do
      } else {
        neighbors.-=(5, 6, 7, 8)
      }
    } else if (value(0) <= points1d(1)) { //it belongs to x2 (5,6,7,8)
      belongs_to.-=(1, 2, 3, 4, 9, 10, 11, 12, 13, 14, 15, 16)
      neighbors.-=(13, 14, 15, 16) //x4 is not neighbor
      if (value(0) <= points1d(0) + range) { //belongs to x1 too
        neighbors.-=(9, 10, 11, 12) //x3 is not neighbor
      } else if (value(0) >= points1d(1) - range) { //belongs to x3 too
        neighbors.-=(1, 2, 3, 4) //x1 is not neighbor
      } else {
        //x1 and x3 are not neighbors
        neighbors.-=(1, 2, 3, 4, 9, 10, 11, 12)
      }
    } else if (value(0) <= points1d(2)) { //it belongs to x3 (9,10,11,12)
      belongs_to.-=(1, 2, 3, 4, 5, 6, 7, 8, 13, 14, 15, 16)
      neighbors.-=(1, 2, 3, 4) //x1 is not neighbor
      if (value(0) <= points1d(1) + range) { //belongs to x2 too
        neighbors.-=(13, 14, 15, 16) //x4 is not neighbor
      } else if (value(0) >= points1d(2) - range) { //belongs to x4 too
        neighbors.-=(5, 6, 7, 8) //x2 is not neighbor
      } else {
        //x2 and x4 are not neighbors
        neighbors.-=(5, 6, 7, 8, 13, 14, 15, 16)
      }
    } else { //it belongs to x4 (13,14,15,16)
      belongs_to.-=(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
      neighbors.-=(1, 2, 3, 4, 5, 6, 7, 8) //x1 and x2 are not neighbors
      if (value(0) <= points1d(2) + range) { //belongs to x3 too
        //nothing to do
      } else { //does not belong to x3
        neighbors.-=(9, 10, 11, 12)
      }
    }
    //2nd dimension=============================
    if (value(1) <= points2d(0)) { //it belongs to y1 (1,5,9,13)
      belongs_to.-=(2, 6, 10, 14, 3, 7, 11, 15, 4, 8, 12, 16)
      neighbors.-=(3, 7, 11, 15, 4, 8, 12, 16) //y3 and y4 are not neighbors
      if (value(1) >= points2d(0) - range) { //belongs to y2 too
        //nothing to do
      } else {
        neighbors.-=(2, 6, 10, 14)
      }
    } else if (value(1) <= points2d(1)) { //it belongs to y2 (2,6,10,14)
      belongs_to.-=(1, 5, 9, 13, 3, 7, 11, 15, 4, 8, 12, 16)
      neighbors.-=(4, 8, 12, 16) //y4 is not neighbor
      if (value(1) <= points2d(0) + range) { //belongs to y1 too
        neighbors.-=(3, 7, 11, 15) //y3 is not neighbor
      } else if (value(1) >= points2d(1) - range) { //belongs to y3 too
        neighbors.-=(1, 5, 9, 13) //y1 is not neighbor
      } else {
        //y1 and y3 are not neighbors
        neighbors.-=(1, 5, 9, 13, 3, 7, 11, 15)
      }
    } else if (value(1) <= points2d(2)) { //it belongs to y3 (3,7,11,15)
      belongs_to.-=(1, 5, 9, 13, 2, 6, 10, 14, 4, 8, 12, 16)
      neighbors.-=(1, 5, 9, 13) //y1 is not neighbor
      if (value(1) <= points2d(1) + range) { //belongs to y2 too
        neighbors.-=(4, 8, 12, 16) //y4 is not neighbor
      } else if (value(1) >= points2d(2) - range) { //belongs to y4 too
        neighbors.-=(2, 6, 10, 14) //y2 is not neighbor
      } else {
        //y2 and y4 are not neighbors
        neighbors.-=(2, 6, 10, 14, 4, 8, 12, 16)
      }
    } else { //it belongs to y4 (4,8,12,16)
      belongs_to.-=(1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15)
      neighbors.-=(1, 5, 9, 13, 2, 6, 10, 14) //y1 and y2 are not neighbors
      if (value(1) <= points2d(2) + range) { //belongs to y3 too
        //nothing to do
      } else { //does not belong to y3
        neighbors.-=(3, 7, 11, 15)
      }
    }

    val partition = belongs_to.head
    neighbors.-=(partition)
    (partition, neighbors)
  }

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

  def combineElementsAdvanced(el1: Data, el2: Data, k: Int): Data = {
    el1.count_after += el2.count_after
    for (elem <- el2.nn_before) {
      el1.insert_nn_before(elem, k)
    }
    el1
  }

  def combineElementsParallel(el1: Data, el2: Data, k: Int): Data = {
    for (elem <- el2.nn_before) {
      el1.insert_nn_before(elem, k)
    }
    el1
  }
}
