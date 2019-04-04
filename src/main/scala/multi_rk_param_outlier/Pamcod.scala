package multi_rk_param_outlier

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import common_utils.Utils._
import common_utils.Data
import outlier.{McodState, MicroCluster}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Pamcod(time_slide: Int, c_queries: List[(Int, Double)]) extends ProcessWindowFunction[(Int, Data), (Long, ListBuffer[MyQuery]), Int, TimeWindow] {

  val slide = time_slide
  val R_distinct_list = c_queries.map(_._2).distinct.sorted
  val k_distinct_list = c_queries.map(_._1).distinct.sorted
  val R_max = R_distinct_list.max
  val R_min = R_distinct_list.min
  val k_max = k_distinct_list.max
  val k_min = k_distinct_list.min
  val k_size = k_distinct_list.size
  val R_size = R_distinct_list.size

  var mc_counter = 1

  lazy val state: ValueState[McodState] = getRuntimeContext
    .getState(new ValueStateDescriptor[McodState]("myState", classOf[McodState]))

  override def process(key: Int, context: Context, elements: Iterable[(Int, Data)], out: Collector[(Long, ListBuffer[MyQuery])]): Unit = {

    val window = context.window

    //create state
    if (state.value == null) {
      val PD = mutable.HashMap[Int, Data]()
      val MC = mutable.HashMap[Int, MicroCluster]()
      val current = McodState(PD, MC)
      state.update(current)
    }

    val all_queries = Array.ofDim[Int](R_size, k_size)

    //insert new elements
    elements
      .filter(_._2.arrival >= window.getEnd - slide)
      .foreach(p => insertPoint(p._2, true))

    //Find outliers
    state.value().PD.values.foreach(p => {
      if (!p.safe_inlier && p.flag == 0) {
        if (p.count_after >= k_max) {
          p.nn_before_set.clear()
          p.safe_inlier = true
        }
        else {
          var i, y: Int = 0
          var b_count = p.nn_before_set.count(p => p._1 >= window.getStart && p._2 <= R_distinct_list(i))
          var a_count = p.count_after_set.count(_ <= R_distinct_list(i))
          var count = b_count + a_count
          do {
            if (count >= k_distinct_list(y)) { //inlier for all i
              y += 1
            } else { //outlier for all y
              for (z <- y until k_size) {
                all_queries(i)(z) += 1
              }
              i += 1
              if (i < R_size) {
                b_count = p.nn_before_set.count(p => p._1 >= window.getStart && p._2 <= R_distinct_list(i))
                a_count = p.count_after_set.count(_ <= R_distinct_list(i))
                count = b_count + a_count
              }
            }
          } while (i < R_size && y < k_size)
        }
      }
    })

    val finalQueries: ListBuffer[MyQuery] = ListBuffer()
    for (i <- 0 until R_size) {
      for (y <- 0 until k_size) {
        finalQueries += MyQuery(R_distinct_list(i), k_distinct_list(y), all_queries(i)(y))
      }
    }

    out.collect((window.getEnd, finalQueries))

    //Remove old points
    var deletedMCs = mutable.HashSet[Int]()
    elements
      .filter(p => p._2.arrival < window.getStart + slide)
      .foreach(p => {
        val delete = deletePoint(p._2)
        if (delete > 0) deletedMCs += delete
      })

    //Delete MCs
    if (deletedMCs.nonEmpty) {
      var reinsert = ListBuffer[Data]()
      deletedMCs.foreach(mc => {
        reinsert = reinsert ++ state.value().MC(mc).points
        state.value().MC.remove(mc)
      })
      val reinsertIndexes = reinsert.sortBy(_.arrival).map(_.id)

      //Reinsert points from deleted MCs
      reinsert.foreach(p => insertPoint(p, false, reinsertIndexes))
    }
  }

  def insertPoint(el: Data, newPoint: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
    var state = this.state.value()
    if (!newPoint) el.clear(-1)
    //Check against MCs on 3 / 2 * R_max
    val closeMCs = findCloseMCs(el)
    //Check if closer MC is within R_min / 2
    val closerMC = if (closeMCs.nonEmpty)
      closeMCs.minBy(_._2)
    else
      (0, Double.MaxValue)
    if (closerMC._2 <= R_min / 2) { //Insert element to MC
      if (newPoint) {
        insertToMC(el, closerMC._1, true)
      }
      else {
        insertToMC(el, closerMC._1, false, reinsert)
      }
    } else { //Check against PD
      val NC = ListBuffer[Data]()
      val NNC = ListBuffer[Data]()
      state.PD.values
        .foreach(p => {
          val thisDistance = distance(el, p)
          if (thisDistance <= 3 * R_max / 2) {
            if (thisDistance <= R_max) {
              addNeighbor(el, p, thisDistance)
              if (newPoint) {
                addNeighbor(p, el, thisDistance)
              }
              else {
                if (reinsert.contains(p.id)) {
                  addNeighbor(p, el, thisDistance)
                }
              }
            }
            if (thisDistance <= R_min / 2) NC += p
            else NNC += p
          }
        })

      if (NC.size >= k_max) { //Create new MC
        createMC(el, NC, NNC)
      }
      else { //Insert in PD
        closeMCs.foreach(mc => el.Rmc += mc._1)
        state.MC.filter(mc => closeMCs.contains(mc._1))
          .foreach(mc => {
            mc._2.points.foreach(p => {
              val thisDistance = distance(el, p)
              if (thisDistance <= R_max) {
                addNeighbor(el, p, thisDistance)
              }
            })
          })
        //Do the skyband
        val tmp_nn_before = kSkyband(k_max - el.count_after - 1, el.nn_before_set)
        el.nn_before_set.clear()
        el.nn_before_set = tmp_nn_before
        state.PD += ((el.id, el))
      }
    }
  }

  def deletePoint(el: Data): Int = {
    var res = 0
    if (el.mc <= 0) { //Delete it from PD
      state.value().PD.remove(el.id)
    } else {
      state.value().MC(el.mc).points -= el
      if (state.value().MC(el.mc).points.size <= k_max) res = el.mc
    }
    res
  }

  def createMC(el: Data, NC: ListBuffer[Data], NNC: ListBuffer[Data]): Unit = {
    NC.foreach(p => {
      p.clear(mc_counter)
      state.value().PD.remove(p.id)
    })
    el.clear(mc_counter)
    NC += el
    val newMC = new MicroCluster(el.value, NC)
    state.value().MC += ((mc_counter, newMC))
    NNC.foreach(p => p.Rmc += mc_counter)
    mc_counter += 1
  }

  def insertToMC(el: Data, mc: Int, update: Boolean, reinsert: ListBuffer[Int] = null): Unit = {
    el.clear(mc)
    state.value().MC(mc).points += el
    if (update) {
      state.value.PD.values.filter(p => p.Rmc.contains(mc)).foreach(p => {
        val thisDistance = distance(p, el)
        if (thisDistance <= R_max) {
          addNeighbor(p, el, thisDistance)
        }
      })
    }
    else {
      state.value.PD.values.filter(p => p.Rmc.contains(mc) && reinsert.contains(p.id)).foreach(p => {
        val thisDistance = distance(p, el)
        if (thisDistance <= R_max) {
          addNeighbor(p, el, thisDistance)
        }
      })
    }
  }

  def findCloseMCs(el: Data): mutable.HashMap[Int, Double] = {
    val res = mutable.HashMap[Int, Double]()
    state.value().MC.foreach(mc => {
      val thisDistance = distance(el, mc._2)
      if (thisDistance <= (3 * R_max) / 2) res.+=((mc._1, thisDistance))
    })
    res
  }

  def addNeighbor(el: Data, neigh: Data, distance: Double): Unit = {
    if (el.arrival > neigh.arrival) {
      el.nn_before_set.+=((neigh.arrival, distance))
    } else {
      el.count_after_set.+=(distance)
      if (distance <= R_min) {
        el.count_after += 1
      }
    }
  }

  def kSkyband(k: Int, neighborsC: ListBuffer[(Long, Double)]): ListBuffer[(Long, Double)] = {
    //neighbors should be in ascending order of distances
    val neighbors = neighborsC.sortBy(_._2)
    val res: ListBuffer[(Long, Double)] = ListBuffer()
    for (i <- neighbors.indices) {
      var counter = 0
      for (y <- 0 until i) {
        if (neighbors(y)._1 > neighbors(i)._1) counter += 1
      }
      if (counter <= k) {
        res.append(neighbors(i))
      }
    }
    res
  }

}
