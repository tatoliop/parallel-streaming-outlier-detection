package outlier

import mtree._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class StateTree(var tree: MTree[Data])

class Advanced(time_slide: Int, range: Double, k: Int) extends ProcessWindowFunction[(Int, Data), Data, Int, TimeWindow] {

  lazy val state: ValueState[StateTree] = getRuntimeContext
    .getState(new ValueStateDescriptor[StateTree]("myTree", classOf[StateTree]))

  override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data)], out: Collector[Data]): Unit = {
    val window = context.window
    //populate Mtree
    var current: StateTree = state.value
    if (current == null) {
      val nonRandomPromotion = new PromotionFunction[Data] {
        /**
          * Chooses (promotes) a pair of objects according to some criteria that is
          * suitable for the application using the M-Tree.
          *
          * @param dataSet          The set of objects to choose a pair from.
          * @param distanceFunction A function that can be used for choosing the
          *                         promoted objects.
          * @return A pair of chosen objects.
          */
        override def process(dataSet: java.util.Set[Data], distanceFunction: DistanceFunction[_ >: Data]): utils.Pair[Data] = {
          utils.Utils.minMax[Data](dataSet)
        }
      }
      val mySplit = new ComposedSplitFunction[Data](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data])
      val myTree = new MTree[Data](k, DistanceFunctions.EUCLIDEAN, mySplit)
      for (el <- elements) {
        myTree.add(el._2)
      }
      current = StateTree(myTree)
    } else {
      elements
        .filter(el => el._2.arrival >= window.getEnd - time_slide)
        .foreach(el => current.tree.add(el._2))
    }

    //Get neighbors
    elements.foreach(p => {
      val tmpData = new Data(p._2.value, p._2.arrival, p._2.flag, p._2.id)
      val query: MTree[Data]#Query = current.tree.getNearestByRange(tmpData, range)
      val iter = query.iterator()
      while (iter.hasNext) {
        val node = iter.next().data
        if (node.id != tmpData.id) {
          if (tmpData.arrival >= window.getEnd - time_slide) {
            if (node.flag == 0) {
              if (node.arrival >= window.getEnd - time_slide) {
                tmpData.count_after += 1
              } else {
                tmpData.insert_nn_before(node.arrival, k)
              }
            }
          } else {
            if (node.arrival >= window.getEnd - time_slide) {
              tmpData.count_after += 1
            }
          }
        }
      }
      out.collect(tmpData)
    })

    //Remove expiring objects from tree and flagged ones
    elements
      .filter(el => el._2.arrival < window.getStart + time_slide || el._2.flag == 1)
      .foreach(el => current.tree.remove(el._2))
    //update state
    state.update(current)
  }
}
