package outlier

import mtree._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import outlier.Utils._

import scala.collection.mutable.ListBuffer

case class MicroCluster(var center: ListBuffer[Double], var points: Int, var id: Int)

case class McodState(var tree: MTree[Data], var PD: ListBuffer[Data], var MC: ListBuffer[MicroCluster])

class Pmcod(time_slide: Int, range: Double, k: Int) extends ProcessWindowFunction[(Int, Data), (Long, Int), Int, TimeWindow] {

  lazy val state: ValueState[McodState] = getRuntimeContext
    .getState(new ValueStateDescriptor[McodState]("myState", classOf[McodState]))

  override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data)], out: Collector[(Long, Int)]): Unit = {

    val window = context.window

    //populate Mtree
    var current: McodState = state.value
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
      val PD = ListBuffer[Data]()
      val MC = ListBuffer[MicroCluster]()
      elements.foreach(p => {
        myTree.add(p._2)
      })
      current = McodState(myTree, PD, MC)
    } else {
      elements
        .filter(el => el._2.arrival >= window.getEnd - time_slide)
        .foreach(el => {
          current.tree.add(el._2)
        })
    }

    //Destroy micro clusters with less than k + 1 points
    var forRemoval = ListBuffer[Int]()
    var elForRemoval = ListBuffer[Int]()
    current.MC.foreach(mymc => {
      if (mymc.points <= k) { //remove MC and reinsert points
        forRemoval.+=(mymc.id)
        elements
          .filter(_._2.mc == mymc.id)
          .foreach(p => {
            elForRemoval.+=(p._2.id)
            p._2.clear(-1)
          })
      }
    })
    forRemoval.foreach(p => {
      val idx = current.MC.indexWhere(_.id == p)
      current.MC.remove(idx)
    })

    var newMCs = scala.collection.mutable.Map[Int, Int]()
    //insert data points from destroyed mcs
    elements
      .filter(p => elForRemoval.contains(p._2.id))
      .foreach(p => {
        val tmpData = p._2
        tmpData.clear(-1)
        if (!newMCs.contains(tmpData.id)) {
          if (current.MC.nonEmpty) { //First check distance to micro clusters
            var min = 2 * range
            var minId = -1
            current.MC.foreach(p => {
              val dist = distance(tmpData, p)
              if (dist <= range / 2 && dist < min) {
                min = dist
                minId = p.id
              }
            })
            if (minId != -1) { //If it belongs to a micro-cluster insert it
              tmpData.clear(minId)
              current.MC.filter(_.id == minId).head.points += 1
            }
          }
          if (tmpData.mc == -1) { //If it doesn't belong to a micro cluster check it against PD and points in MCs
            var count = 0
            //vars for forming a new mc
            var idMC = 0
            var NC = ListBuffer[Int]()
            if (current.MC.nonEmpty) idMC = current.MC.map(_.id).max //take the max id of current micro clusters
            //range query
            val query: MTree[Data]#Query = current.tree.getNearestByRange(tmpData, range)
            val iter = query.iterator()
            while (iter.hasNext) {
              val node = iter.next().data
              if (node.id != tmpData.id) {
                if (node.arrival < window.getEnd - time_slide) { //change only with old neighbors
                  if (tmpData.arrival >= node.arrival) {
                    tmpData.insert_nn_before(node.arrival, k)
                  } else {
                    tmpData.count_after += 1
                  }
                }
              }
            }
            if (NC.size >= k) { //create new MC
              val newMC = new MicroCluster(tmpData.value, NC.size + 1, idMC + 1)
              current.MC.+=(newMC)
              tmpData.clear(idMC + 1)
              NC.foreach(p => { //Remove points from PD
                newMCs += (p -> (idMC + 1))
                val idx = current.PD.indexWhere(_.id == p)
                current.PD.remove(idx)
              })
            } else { //Update PD
              current.PD.+=(tmpData)
            }
          }
        }
      })

    newMCs.foreach(p => { //insert data points to new MCs
      elements.filter(_._2.id == p._1).head._2.clear(p._2)
    })
    newMCs.clear()

    //insert new points
    elements
      .filter(p => p._2.arrival >= window.getEnd - time_slide)
      .foreach(p => { //For each new data point
        val tmpData = p._2
        if (!newMCs.contains(tmpData.id)) {
          if (current.MC.nonEmpty) { //First check distance to micro clusters
            var min = 2 * range
            var minId = -1
            current.MC.foreach(p => {
              val dist = distance(tmpData, p)
              if (dist <= range / 2 && dist < min) {
                min = dist
                minId = p.id
              }
            })
            if (minId != -1) { //If it belongs to a micro-cluster insert it
              tmpData.clear(minId)
              current.MC.filter(_.id == minId).head.points += 1
              //compute vs PD and update PD
              current.PD.foreach(p => {
                if (p.arrival < window.getEnd - time_slide) {
                  val dist = distance(tmpData, p)
                  if (dist <= range) {
                    p.count_after += 1
                  }
                }
              })
            }
          }
          if (tmpData.mc == -1) { //If it doesn't belong to a micro cluster check it against PD and points in MCs
            //vars for forming a new mc
            var idMC = 0
            var NC = ListBuffer[Int]()
            if (current.MC.nonEmpty) idMC = current.MC.map(_.id).max //take the max id of current micro clusters
            //range query

            val query: MTree[Data]#Query = current.tree.getNearestByRange(tmpData, range)
            val iter = query.iterator()

            while (iter.hasNext) {
              val node = iter.next().data
              if (node.id != tmpData.id) {
                if (tmpData.arrival >= node.arrival) {
                  tmpData.insert_nn_before(node.arrival, k)
                } else {
                  tmpData.count_after += 1
                }
                if (current.PD.count(_.id == node.id) == 1) { //Update all PD metadata

                  val dist = distance(tmpData, node)
                  if (dist <= range / 2) NC.+=(node.id) //Possible new micro cluster

                  if (node.arrival < window.getEnd - time_slide) {
                    if (tmpData.arrival >= node.arrival) {
                      current.PD.filter(_.id == node.id).head.count_after += 1
                    } else {
                      current.PD.filter(_.id == node.id).head.insert_nn_before(tmpData.arrival, k)
                    }
                  }
                }
              }
            }
            if (NC.size >= k) { //create new MC
              val newMC = new MicroCluster(tmpData.value, NC.size + 1, idMC + 1)
              current.MC.+=(newMC)
              tmpData.clear(idMC + 1)
              NC.foreach(p => { //Remove points from PD
                newMCs += (p -> (idMC + 1))
                val idx = current.PD.indexWhere(_.id == p)
                current.PD.remove(idx)
              })
            } else { //Update PD
              current.PD.+=(tmpData)
            }

          }
        }
      })

    newMCs.foreach(p => {
      elements.filter(_._2.id == p._1).head._2.clear(p._2)
    })

    var outliers = ListBuffer[Int]()
    //Find outliers
    current.PD.filter(_.flag == 0).foreach(p => {
      val nnBefore = p.nn_before.count(_ >= window.getStart)
      if (nnBefore + p.count_after < k) outliers += p.id
    })

    out.collect((window.getEnd, outliers.size))

    //Remove expiring objects from tree and PD/MC
    elements
      .filter(el => el._2.arrival < window.getStart + time_slide)
      .foreach(el => {
        current.tree.remove(el._2)
        if (el._2.mc == -1) {
          val index = current.PD.indexWhere(_.id == el._2.id)
          current.PD.remove(index)
        } else {
          current.MC.filter(_.id == el._2.mc).head.points -= 1
        }
      })

    //update state
    state.update(current)
  }


}

