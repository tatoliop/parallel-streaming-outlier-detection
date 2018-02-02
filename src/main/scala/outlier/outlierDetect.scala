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
  val count_slide_percent: Double = 100 * (count_slide.toDouble / count_window)
  //time window variables
  val time_window: Int = count_window / 10
  val time_slide: Int = (time_window * (count_slide_percent / 100)).toInt
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
    12 -> "90.7!95.965!97.633!98.75!99.7!100.37!101.49!102.84!104.25!105.59!108.36",
    16 -> "87.231!94.222!96.5!97.633!98.5!99.25!99.897!100.37!101.16!102.13!103.18!104.25!105.25!106.65!109.75")

  var id = 0

  def main(args: Array[String]) {

    if (args.length != 4) {
      println("Wrong arguments!")
      System.exit(1)
    } else if (args(0).toInt != 8 && args(0).toInt != 12 && args(0).toInt != 16) {
      println("Parallelism should be 8, 12 or 16!")
      System.exit(1)
    }

    parallelism = args(0).toInt
    count_window = args(1).toInt
    count_slide = args(2).toInt
    data_input = args(3)
    var points_string = List[String]()
    if (parallelism == 8) {
      //points [0-6]
      points_string = spatial(8).split("!").toList
    } else if (parallelism == 12) {
      points_string = spatial(12).split("!").toList
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
        var list = new ListBuffer[(Int, Data1d)]
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
        val tmpEl = (belongs_to, new Data1d(value, new_time, 0, id))
        list.+=(tmpEl)
        if (previous != -1) {
          val tmpEl2 = (previous, new Data1d(value, new_time, 1, id))
          list.+=(tmpEl2)
        }
        if (next != -1) {
          val tmpEl2 = (next, new Data1d(value, new_time, 1, id))
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
      .evictor(new StormEvictor)
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

  class StormTimestamp extends AssignerWithPeriodicWatermarks[(Int, Data1d)] with Serializable {

    val maxOutOfOrderness = 1000L // 1 seconds

    override def extractTimestamp(e: (Int, Data1d), prevElementTimestamp: Long) = {
      val timestamp = e._2.arrival
      timestamp
    }

    override def getCurrentWatermark(): Watermark = {
      new Watermark(System.currentTimeMillis - maxOutOfOrderness)
    }
  }

  class StormEvictor extends Evictor[(Int, Data1d), TimeWindow] {
    override def evictBefore(elements: Iterable[TimestampedValue[(Int, Data1d)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      val iteratorEl = elements.iterator
      while (iteratorEl.hasNext) {
        val tmpNode = iteratorEl.next().getValue._2
        if (tmpNode.flag == 1 && tmpNode.arrival >= window.getStart && tmpNode.arrival < window.getEnd - time_slide) iteratorEl.remove()
      }
    }

    override def evictAfter(elements: Iterable[TimestampedValue[(Int, Data1d)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
    }
  }

  case class StateTree(var tree: MTree[Data1d])

  class ExactStorm extends ProcessWindowFunction[(Int, Data1d), (Long, Int), Int, TimeWindow] {

    lazy val state: ValueState[StateTree] = getRuntimeContext
      .getState(new ValueStateDescriptor[StateTree]("myTree", classOf[StateTree]))

    override def process(key: Int, context: Context, elements: scala.Iterable[(Int, Data1d)], out: Collector[(Long, Int)]): Unit = {
      val time1 = System.currentTimeMillis()
      val window = context.window
      //populate Mtree
      var current: StateTree = state.value
      if (current == null) {
        val nonRandomPromotion = new PromotionFunction[Data1d] {
          /**
            * Chooses (promotes) a pair of objects according to some criteria that is
            * suitable for the application using the M-Tree.
            *
            * @param dataSet          The set of objects to choose a pair from.
            * @param distanceFunction A function that can be used for choosing the
            *                         promoted objects.
            * @return A pair of chosen objects.
            */
          override def process(dataSet: util.Set[Data1d], distanceFunction: DistanceFunction[_ >: Data1d]): utils.Pair[Data1d] = {
            utils.Utils.minMax[Data1d](dataSet)
          }
        }
        val mySplit = new ComposedSplitFunction[Data1d](nonRandomPromotion, new PartitionFunctions.BalancedPartition[Data1d])
        val myTree = new MTree[Data1d](k, count_window + count_slide, DistanceFunctions.EUCLIDEAN, mySplit)
        for (el <- elements) {
          myTree.add(el._2)
        }
        current = StateTree(myTree)
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
          val tmpData = new StormData(p._2)
          val query: MTree[Data1d]#Query = current.tree.getNearestByRange(tmpData, range)
          val iter = query.iterator()
          while (iter.hasNext) {
            val node = iter.next().data
            if (node.id != tmpData.id) {
              if(node.arrival >= tmpData.arrival){
                tmpData.count_after += 1
              }else{
                tmpData.insert_nn_before(node.arrival, k)
              }
            }
          }
          val nnBefore = tmpData.nn_before.count(_ > window.getEnd - time_window)
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

  class ShowOutliers extends ProcessWindowFunction[(Long, Int), String, Long, TimeWindow] {

    override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Int)], out: Collector[String]): Unit = {
      val outliers = elements.toList.map(_._2).sum
      out.collect(s"$key;$outliers")
    }
  }

}
