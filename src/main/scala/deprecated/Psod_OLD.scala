//package multi_param_outlier
//
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//import common_utils.Utils._
//import common_utils.Data
//
//import scala.collection.mutable
//import scala.collection.mutable.ListBuffer
//
//case class PsodState(var index: mutable.LinkedHashMap[Int, Data])
//
//class Psod_OLD(time_slide: Int, c_queries: List[(Int, Double)]) extends ProcessWindowFunction[(Int, Data), (Long, ListBuffer[MyQuery]), Int, TimeWindow] {
//
//  val slide = time_slide
//  val R_distinct_list = c_queries.map(_._2).distinct.sorted
//  val k_distinct_list = c_queries.map(_._1).distinct.sorted
//  val R_max = R_distinct_list.max
//  val R_min = R_distinct_list.min
//  val k_max = k_distinct_list.max
//  val k_min = k_distinct_list.min
//  val k_size = k_distinct_list.size
//  val R_size = R_distinct_list.size
//
//  lazy val state: ValueState[PsodState] = getRuntimeContext
//    .getState(new ValueStateDescriptor[PsodState]("myState", classOf[PsodState]))
//
//  override def process(key: Int, context: Context, elements: Iterable[(Int, Data)], out: Collector[(Long, ListBuffer[MyQuery])]): Unit = {
//
//    val window = context.window
//
//    //Create state
//    if (state.value == null) {
//      val index = mutable.LinkedHashMap[Int, Data]()
//      val current = PsodState(index)
//      state.update(current)
//    }
//
//    val all_queries = Array.ofDim[Int](R_size, k_size)
//
//    //Insert new elements to state
//    elements
//      .filter(_._2.arrival >= window.getEnd - slide)
//      .foreach(p => {
//        insertPoint(p._2)
//      })
//
//    //Update elements
//    elements.foreach(p => {
//      if (!p._2.safe_inlier && p._2.flag == 0) {
//        if (p._2.lSky.getOrElse(0, ListBuffer()).count(_._2 >= p._2.arrival) >= k_max) p._2.safe_inlier = true
//        else {
//          var i, y: Int = 0
//          var count = p._2.lSky.getOrElse(i, ListBuffer()).count(_._2 >= window.getStart)
//          do{
//            if(count >= k_distinct_list(y)){ //inlier for all i
//              y += 1
//            }else{ //outlier for all y
//              for(z <- y until k_size){
//                all_queries(i)(z) += 1
//              }
//              i += 1
//              count += p._2.lSky.getOrElse(i, ListBuffer()).count(_._2 >= window.getStart)
//            }
//          }while(i < R_size && y < k_size)
//        }
//      }
//    })
//
//    val finalQueries: ListBuffer[MyQuery] = ListBuffer()
//    for (i <- 0 until R_size){
//      for (y <- 0 until k_size){
//        finalQueries += MyQuery(R_distinct_list(i), k_distinct_list(y), all_queries(i)(y))
//      }
//    }
//
//    //Output
//    out.collect((window.getEnd, finalQueries))
//
//    //Remove old points
//    elements
//      .filter(p => p._2.arrival < window.getStart + slide)
//      .foreach(p => {
//        deletePoint(p._2)
//      })
//  }
//
//  //DONE
//  def insertPoint(el: Data): Unit = {
//    state.value().index.values
//      .foreach(p => {
//        val thisDistance = distance(el, p)
//        if (thisDistance <= R_max) {
//          addNeighbor(el, p, thisDistance)
//        }
//      })
//    state.value().index += ((el.id, el))
//  }
//
//  //DONE
//  def deletePoint(el: Data): Unit = {
//    state.value().index.remove(el.id)
//  }
//
//  //DONE
//  def addNeighbor(el: Data, neigh: Data, distance: Double): Unit = {
//    val norm_dist = normalizeDistance(distance)
//    if(el.flag == 0)
//      el.lSky.update(norm_dist, el.lSky.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((neigh.id, neigh.arrival)))
//    if (!neigh.safe_inlier && neigh.flag == 0)
//      neigh.lSky.update(norm_dist, neigh.lSky.getOrElse(norm_dist, ListBuffer[(Int, Long)]()) += ((el.id, el.arrival)))
//  }
//
//  //DONE
//  def normalizeDistance(distance: Double): Int = {
//    var res = -1
//    var i = 0
//    do {
//      if (distance <= R_distinct_list(i)) res = i
//      i += 1
//    } while (i < R_distinct_list.size && res == -1)
//    res
//  }
//
//}
