package outlier

import mtree._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class AdvancedVPState(var tree: MTree[Data])

class AdvancedVP(time_slide: Int, range: Double, k: Int) extends ProcessWindowFunction[(Int, Data), (Long, Int), Int, TimeWindow] {

  lazy val state: ValueState[AdvancedVPState] = getRuntimeContext
    .getState(new ValueStateDescriptor[AdvancedVPState]("myTree", classOf[AdvancedVPState]))

  override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data)], out: Collector[(Long, Int)]): Unit = {
    val window = context.window
    //populate Mtree
    var current: AdvancedVPState = state.value
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
      current = AdvancedVPState(myTree)
    } else {
      elements
        .filter(el => el._2.arrival >= window.getEnd - time_slide)
        .foreach(el => current.tree.add(el._2))
    }

    //Variable for number of outliers
    var outliers = 0

    //Get neighbors
    elements
      .filter(_._2.flag == 0)
      .foreach(p => {
        val tmpData = new Data(p._2.value, p._2.arrival, p._2.flag, p._2.id)
        val query: MTree[Data]#Query = current.tree.getNearestByRange(tmpData, range)
        val iter = query.iterator()
        while (iter.hasNext) {
          val node = iter.next().data
          if (node.id != tmpData.id) {
            if (node.arrival >= tmpData.arrival) {
              tmpData.count_after += 1
            } else {
              tmpData.insert_nn_before(node.arrival, k)
            }
          }
        }

        val nnBefore = tmpData.nn_before.count(_ >= window.getStart)
        if (nnBefore + tmpData.count_after < k) outliers += 1
      })
    out.collect((window.getEnd, outliers))


    //Remove expiring objects from tree and flagged ones
    elements
      .filter(el => el._2.arrival < window.getStart + time_slide)
      .foreach(el => current.tree.remove(el._2))
    //update state
    state.update(current)
  }
}

