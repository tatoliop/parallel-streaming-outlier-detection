package outlier

import java.lang.Iterable
import java.util
import javax.management.Query

import mtree._
import org.apache.flink.api.common.ExecutionMode
import org.apache.flink.api.common.functions.{FlatMapFunction, Partitioner}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks._

object outlierDetect {

  //data input
  var data_input: String = "DummyData/stock/stock_id_20k.txt"
  //partitioning
  var parallelism: Int = 8
  //count window variables (total / partitions)
  var count_window: Int = 10000
  var count_slide: Int = 500
  var count_slide_percent: Double = 100 * (count_slide.toDouble / count_window)
  //time window variables
  var time_window: Int = count_window / 10
  var time_slide: Int = (time_window * (count_slide_percent / 100)).toInt
  //distance outlier variables
  val k: Int = 50
  val range: Double = 0.45
  //source variables
  val randomGenerate: Int = 100
  val stopStreamAt: Int = 2000
  //stats
  var times_per_slide = Map[String, Long]()
  //helper to slow down stream
  val cur_time = System.currentTimeMillis() + 1000000L //some delay for the correct timestamp
  //hardcoded spatial partitioning
  val spatial = Map[Int, String](8 -> "94.222!97.633!99.25!100.37!102.13!104.25!106.65",
    32 -> "77.457!87.231!91.88!94.222!95.59!96.5!97.125!97.633!98.074!98.5!98.888!99.25!99.588!99.897!100.07!100.37!100.72!101.16!101.65!102.13!102.65!103.18!103.72!104.25!104.78!105.25!105.79!106.65!107.84!109.75!112.14",
    16 -> "87.231!94.222!96.5!97.633!98.5!99.25!99.897!100.37!101.16!102.13!103.18!104.25!105.25!106.65!109.75")

  var id = 0

  def main(args: Array[String]) {

    if (args.length != 4) {
      println("Wrong arguments!")
      System.exit(1)
    } else if (args(0).toInt != 8 && args(0).toInt != 32 && args(0).toInt != 16) {
      println("Parallelism should be 8, 32 or 16!")
      System.exit(1)
    }

    parallelism = args(0).toInt
    count_window = args(1).toInt
    count_slide = args(2).toInt
    data_input = args(3)
    count_slide_percent = 100 * (count_slide.toDouble / count_window)
    //time window variables
    time_window = count_window / 10
    time_slide = (time_window * (count_slide_percent / 100)).toInt

    var points_string = List[String]()
    if (parallelism == 8) {
      points_string = spatial(8).split("!").toList
    } else if (parallelism == 32) {
      points_string = spatial(32).split("!").toList
    } else if (parallelism == 16) {
      points_string = spatial(16).split("!").toList
    }
    val points = points_string.map(_.toDouble)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val data = env.readTextFile(data_input)
    val mappedData = data
      .flatMap(line => {
        val splitLine = line.split("&")
        val id = splitLine(0).toInt
        val value = splitLine(1).toDouble
        val multiplication = id / count_slide
        val new_time: Long = cur_time + (multiplication * time_slide)
        var list = new ListBuffer[(Int, StormData)]
        var i = 0
        var break = false
        var belongs_to, previous, next = -1
        do {
          if (value <= points(i)) {
            belongs_to = i //belongs to the current partition
            break = true
            if (i != 0) {
              //check if it is near the previous partition
              if (value <= points(i - 1) + range) {
                previous = i - 1
              }
            } //check if it is near the next partition
            if (value >= points(i) - range) {
              next = i + 1
            }
          }
          i += 1
        } while (i <= parallelism - 2 && !break)
        if (!break) {
          // it belongs to the last partition
          belongs_to = parallelism - 1
          if (value <= points(parallelism - 2) + range) {
            previous = parallelism - 2
          }
        }
        val tmpEl = (belongs_to, new StormData(value, new_time, 0, id))
        list.+=(tmpEl)
        if (previous != -1) {
          val tmpEl2 = (previous, new StormData(value, new_time, 1, id))
          list.+=(tmpEl2)
        }
        if (next != -1) {
          val tmpEl2 = (next, new StormData(value, new_time, 1, id))
          list.+=(tmpEl2)
        }
        list
      })

    val timestampData = mappedData
      .assignTimestampsAndWatermarks(new StormTimestamp)

    val keyedData = timestampData
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
      .allowedLateness(Time.milliseconds(1000))
      .process(new ExactStorm)

    val groupedOutliers = keyedData
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(time_slide))
      .process(new ShowOutliers)

    groupedOutliers.print()
    println("Starting outlier test")

    env.execute("Outlier-flink")

    println("Finished outlier test")
  }

  class StormTimestamp extends AssignerWithPeriodicWatermarks[(Int, StormData)] with Serializable {

    val maxOutOfOrderness = 1000L // 1 seconds

    override def extractTimestamp(e: (Int, StormData), prevElementTimestamp: Long) = {
      val timestamp = e._2.arrival
      timestamp
    }

    override def getCurrentWatermark(): Watermark = {
      new Watermark(System.currentTimeMillis - maxOutOfOrderness)
    }
  }

  case class MicroCluster(var center: Double, var points: Int, var id: Int)

  case class StateTree(var tree: MTree[StormData], var PD: ListBuffer[StormData], var MC: ListBuffer[MicroCluster])

  class ExactStorm extends ProcessWindowFunction[(Int, StormData), (Long, Int), Int, TimeWindow] {

    lazy val state: ValueState[StateTree] = getRuntimeContext
      .getState(new ValueStateDescriptor[StateTree]("myState", classOf[StateTree]))

    override def process(key: Int, context: Context, elements: scala.Iterable[(Int, StormData)], out: Collector[(Long, Int)]): Unit = {
      val window = context.window
      //populate Mtree
      var current: StateTree = state.value
      if (current == null) {
        val nonRandomPromotion = new PromotionFunction[StormData] {
          /**
            * Chooses (promotes) a pair of objects according to some criteria that is
            * suitable for the application using the M-Tree.
            *
            * @param dataSet          The set of objects to choose a pair from.
            * @param distanceFunction A function that can be used for choosing the
            *                         promoted objects.
            * @return A pair of chosen objects.
            */
          override def process(dataSet: util.Set[StormData], distanceFunction: DistanceFunction[_ >: StormData]): utils.Pair[StormData] = {
            utils.Utils.minMax[StormData](dataSet)
          }
        }
        val mySplit = new ComposedSplitFunction[StormData](nonRandomPromotion, new PartitionFunctions.BalancedPartition[StormData])
        val myTree = new MTree[StormData](k, 2 * count_window, DistanceFunctions.EUCLIDEAN, mySplit)
        val PD = ListBuffer[StormData]()
        val MC = ListBuffer[MicroCluster]()
        elements.foreach(p => {
          myTree.add(p._2)
        })
        current = StateTree(myTree, PD, MC)
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
              val query: MTree[StormData]#Query = current.tree.getNearestByRange(tmpData, range)
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
                //TO ELEGXO ME TO PD KAI ENIMERONO TO PD
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
              val query: MTree[StormData]#Query = current.tree.getNearestByRange(tmpData, range)
              val iter = query.iterator()
              while (iter.hasNext) {
                val node = iter.next().data
                if (node.id != tmpData.id) {
                  if (tmpData.arrival >= node.arrival) {
                    tmpData.insert_nn_before(node.arrival, k)
                  } else {
                    tmpData.count_after += 1
                  }
                  if (current.PD.contains(node)) { //Update all PD metadata
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

    def distance(xs: StormData, ys: StormData): Double = {
      val value = scala.math.pow(xs.value - ys.value, 2)
      val res = scala.math.sqrt(value)
      res
    }

    def distance(xs: StormData, ys: MicroCluster): Double = {
      val value = scala.math.pow(xs.value - ys.center, 2)
      val res = scala.math.sqrt(value)
      res
    }
  }

  class ShowOutliers extends ProcessWindowFunction[(Long, Int), String, Long, TimeWindow] {

    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Int)], out: Collector[String]): Unit = {
      val outliers = elements.toList.map(_._2).sum
      out.collect(s"$key;$outliers")
    }
  }

}
