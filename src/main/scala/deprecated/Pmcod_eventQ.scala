//package outlier
//
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//import outlier.Utils._
//import java.util.PriorityQueue
//import java.util.Comparator
//
//import scala.collection.mutable
//import scala.collection.mutable.ListBuffer
//
//case class McodState(var PD: mutable.HashMap[Int, Data], var MC: mutable.HashMap[Int, MicroCluster], eventQ: PriorityQueue[EventQElement])
//
//case class MicroCluster(var center: ListBuffer[Double], var points: ListBuffer[Data])
//
//class EventQElement(myId: Int, myTime: Long) {
//  val id: Int = myId
//  val time: Long = myTime
//
//  def canEqual(other: Any): Boolean = {
//    other.isInstanceOf[EventQElement]
//  }
//
//  override def equals(other: Any): Boolean = {
//    other match {
//      case that: EventQElement =>
//        id == that.id
//      case _ => false
//    }
//  }
//
//  override def hashCode(): Int = {
//    val state = Seq(id)
//    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
//  }
//
//  override def toString = s"EventQElement($id, $time)"
//}
//
//class Pmcod_eventQ(time_slide: Int, range: Double, k_c: Int) extends ProcessWindowFunction[(Int, Data), (Long, Int), Int, TimeWindow] {
//
//  val slide: Int = time_slide
//  val R: Double = range
//  val k: Int = k_c
//  var mc_counter: Int = 1
//
//  lazy val state: ValueState[McodState] = getRuntimeContext
//    .getState(new ValueStateDescriptor[McodState]("myState", classOf[McodState]))
//
//  override def process(key: Int, context: Context, elements: Iterable[(Int, Data)], out: Collector[(Long, Int)]): Unit = {
//
//    val window = context.window
//
//    //create state
//    if (state.value == null) {
//      val PD = mutable.HashMap[Int, Data]()
//      val MC = mutable.HashMap[Int, MicroCluster]()
//      val EventQComparator: Comparator[EventQElement] = new Comparator[EventQElement]() {
//        override def compare(s1: EventQElement, s2: EventQElement): Int = (s1.time - s2.time).toInt
//      }
//      val eventQ = new PriorityQueue[EventQElement](EventQComparator)
//      val current = McodState(PD, MC, eventQ)
//
//      state.update(current)
//    }
//
//    //insert new elements
//    elements
//      .filter(_._2.arrival >= window.getEnd - slide)
//      .foreach(p => insertPoint(p._2, true, window.getStart))
//
//    //Find outliers
//    val outliers = state.value().PD.values.count(p => p.flag == 0 && p.node_type == NodeType.OUTLIER)
//
//    out.collect((window.getEnd, outliers))
//
//    //Remove old points
//    var deletedMCs = mutable.HashSet[Int]()
//    elements
//      .filter(p => p._2.arrival < window.getStart + slide)
//      .foreach(p => {
//        val delete = deletePoint(p._2)
//        if (delete > 0) deletedMCs += delete
//      })
//
//    //Delete MCs
//    if (deletedMCs.nonEmpty) {
//      var reinsert = ListBuffer[Data]()
//      deletedMCs.foreach(mc => {
//        reinsert = reinsert ++ state.value().MC(mc).points
//        state.value().MC.remove(mc)
//      })
//      val reinsertIndexes = reinsert.map(_.id)
//
//      //Reinsert points from deleted MCs
//      reinsert.foreach(p => insertPoint(p, false, window.getStart, reinsertIndexes))
//    }
//    //update eventQ
//    val expiringSlide = window.getStart + slide
//    while (state.value().eventQ.size() > 0 && state.value().eventQ.peek().time < expiringSlide) {
//      val topQ = state.value().eventQ.poll()
//      val el = state.value().PD.getOrElse(topQ.id, null)
//      if (el != null) {
//        if (el.node_type == NodeType.INLIER_PD) {
//          val before = el.nn_before.count(_ >= expiringSlide)
//          if (el.count_after + before >= k) {
//            val newTime = el.get_min_nn_before(expiringSlide)
//            if (newTime != 0L)
//              state.value().eventQ.add(new EventQElement(topQ.id, newTime))
//          } else {
//            el.node_type = NodeType.OUTLIER
//          }
//        }
//      }
//    }
//  }
//
//  def insertPoint(el: Data, newPoint: Boolean, windowStart: Long, reinsert: ListBuffer[Int] = null): Unit = {
//    var state = this.state.value()
//    if (!newPoint) {
//      el.clear(-1)
//      el.node_type = null
//    }
//    //Check against MCs on 3/2R
//    val closeMCs = findCloseMCs(el)
//    //Check if closer MC is within R/2
//    val closerMC = if (closeMCs.nonEmpty)
//      closeMCs.minBy(_._2)
//    else
//      (0, Double.MaxValue)
//    if (closerMC._2 <= R / 2) { //Insert element to MC
//      if (newPoint) {
//        insertToMC(el, closerMC._1, true, windowStart)
//      }
//      else {
//        insertToMC(el, closerMC._1, false, windowStart, reinsert)
//      }
//    }
//    else { //Check against PD
//      val NC = ListBuffer[Data]()
//      val NNC = ListBuffer[Data]()
//
//      state.PD.values
//        .foreach(p => {
//          val thisDistance = distance(el, p)
//          if (thisDistance <= 3 * R / 2) {
//            if (thisDistance <= R) { //Update metadata
//              addNeighbor(el, p)
//              if (newPoint) {
//                addNeighbor(p, el, windowStart)
//              }
//              else {
//                if (reinsert.contains(p.id)) {
//                  addNeighbor(p, el, windowStart)
//                }
//              }
//            }
//            if (thisDistance <= R / 2) NC += p
//            else NNC += p
//          }
//        })
//
//      if (NC.size >= k) { //Create new MC
//        createMC(el, NC, NNC)
//      }
//      else { //Insert in PD
//        closeMCs.foreach(mc => el.Rmc += mc._1)
//        state.MC.filter(mc => closeMCs.contains(mc._1))
//          .foreach(mc => {
//            mc._2.points.foreach(p => {
//              val thisDistance = distance(el, p)
//              if (thisDistance <= R) {
//                addNeighbor(el, p)
//              }
//            })
//          })
//        val after = el.count_after
//        if (after >= k) el.node_type = NodeType.SAFE_INLIER
//        else {
//          val before = el.nn_before.count(_ >= windowStart)
//          if (after + before >= k) {
//            el.node_type = NodeType.INLIER_PD
//            val tmin = el.get_min_nn_before(windowStart)
//            if (tmin != 0L) {
//              state.eventQ.add(new EventQElement(el.id, tmin))
//            }
//          } else {
//            el.node_type = NodeType.OUTLIER
//          }
//        }
//        state.PD += ((el.id, el))
//      }
//    }
//  }
//
//  def deletePoint(el: Data): Int = {
//    var res = 0
//    if (el.mc <= 0) { //Delete it from PD
//      state.value().PD.remove(el.id)
//      //state.value().eventQ.remove(new EventQElement(el.id, 0L))
//    } else {
//      state.value().MC(el.mc).points -= el
//      if (state.value().MC(el.mc).points.size <= k) res = el.mc
//    }
//    res
//  }
//
//  def createMC(el: Data, NC: ListBuffer[Data], NNC: ListBuffer[Data]): Unit = {
//    NC.foreach(p => {
//      p.clear(mc_counter)
//      p.node_type = NodeType.INLIER_MC
//      state.value().PD.remove(p.id)
//      //state.value().eventQ.remove(new EventQElement(p.id, 0L))
//    })
//    el.clear(mc_counter)
//    el.node_type = NodeType.INLIER_MC
//    NC += el
//    val newMC = new MicroCluster(el.value, NC)
//    state.value().MC += ((mc_counter, newMC))
//    NNC.foreach(p => p.Rmc += mc_counter)
//    mc_counter += 1
//  }
//
//  def insertToMC(el: Data, mc: Int, update: Boolean, windowStart: Long, reinsert: ListBuffer[Int] = null): Unit = {
//    el.clear(mc)
//    el.node_type = NodeType.INLIER_MC
//    state.value().MC(mc).points += el
//    if (update) {
//      state.value.PD.values.filter(p => p.Rmc.contains(mc)).foreach(p => {
//        if (distance(p, el) <= R) {
//          addNeighbor(p, el, windowStart)
//        }
//      })
//    }
//    else {
//      state.value.PD.values.filter(p => p.Rmc.contains(mc) && reinsert.contains(p.id)).foreach(p => {
//        if (distance(p, el) <= R) {
//          addNeighbor(p, el, windowStart)
//        }
//      })
//    }
//  }
//
//  def findCloseMCs(el: Data): mutable.HashMap[Int, Double] = {
//    val res = mutable.HashMap[Int, Double]()
//    state.value().MC.foreach(mc => {
//      val thisDistance = distance(el, mc._2)
//      if (thisDistance <= (3 * R) / 2) res.+=((mc._1, thisDistance))
//    })
//    res
//  }
//
//  def addNeighbor(el: Data, neigh: Data, windowStart: Long = 0L): Unit = {
//    if (el.arrival > neigh.arrival) {
//      el.insert_nn_before(neigh.arrival, k)
//      if (windowStart != 0L && el.node_type == NodeType.INLIER_PD) {
//        val tmin = el.get_min_nn_before(windowStart)
//        if (tmin != 0L) {
//          //state.value().eventQ.remove(new EventQElement(el.id, 0L))
//          state.value().eventQ.add(new EventQElement(el.id, tmin))
//        }
//      }
//    } else {
//      el.count_after += 1
//      if (windowStart != 0L && el.count_after >= k) {
//        el.node_type = NodeType.SAFE_INLIER
//        //state.value().eventQ.remove(new EventQElement(el.id, 0L))
//      }
//    }
//    if (windowStart != 0L && el.node_type == NodeType.OUTLIER) {
//      val after = el.count_after
//      val before = el.nn_before.count(_ >= windowStart)
//      if (before + after >= k) {
//        el.node_type = NodeType.INLIER_PD
//        val tmin = el.get_min_nn_before(windowStart)
//        if (tmin != 0L) {
//          state.value().eventQ.add(new EventQElement(el.id, tmin))
//        }
//      }
//    }
//  }
//
//}
