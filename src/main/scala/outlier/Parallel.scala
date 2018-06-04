package outlier

import mtree._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import Utils._

class Parallel(time_slide: Int, range: Double, k: Int) extends ProcessWindowFunction[(Int, Data), Data, Int, TimeWindow] {

  override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data)], out: Collector[Data]): Unit = {
    val window = context.window
    val inputList = elements.map(_._2).toList

    inputList.filter(_.arrival >= window.getEnd - time_slide).foreach(p => {
      refreshList(p, inputList, context.window)
    })
    inputList.foreach(p => {
      if (!p.safe_inlier) {
        out.collect(p)
      }
    })
  }

  def refreshList(node: Data, nodes: List[Data], window: TimeWindow): Unit = {
    if (nodes.nonEmpty) {
      val neighbors = nodes
        .filter(_.id != node.id)
        .map(x => (x, distance(x, node)))
        .filter(_._2 <= range).map(_._1)

      neighbors
        .foreach(x => {
          if (x.arrival < window.getEnd - time_slide) {
            node.insert_nn_before(x.arrival, k)
          } else {
            node.count_after += 1
            if (node.count_after >= k) {
              node.safe_inlier = true
            }
          }
        })

      nodes
        .filter(x => x.arrival < window.getEnd - time_slide && neighbors.contains(x))
        .foreach(n => {
          n.count_after += 1
          if (n.count_after >= k) {
            n.safe_inlier = true
          }
        }) //add new neighbor to previous nodes
    }
  }

}