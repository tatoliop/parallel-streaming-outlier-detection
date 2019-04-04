package outlier

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import common_utils.Algorithms._
import common_utils.Partitioning._

import scala.collection.mutable.ListBuffer

object OutlierDetect {

  //helper to slow down stream
  val cur_time = System.currentTimeMillis() + 1000000L //some delay for the correct timestamp

  def main(args: Array[String]) {
    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    //Parameters
    val input = parameters.getRequired("input")
    val treeInput = parameters.get("treeInput", "")
    val count_window = parameters.getRequired("window").toInt
    val count_slide = parameters.getRequired("slide").toInt
    val dataset = parameters.getRequired("dataset")
    val algorithm = parameters.getRequired("algorithm")
    val partitioning_type = parameters.get("part", "grid")
    val metric_count = parameters.get("VPcount", "10000").toInt
    val parallelism = parameters.getRequired("parallelism").toInt
    val k = parameters.getRequired("k").toInt
    val range = parameters.getRequired("range").toDouble

    val myVPTree =
      if (partitioning_type == "metric") {
        createVPtree(metric_count, parallelism, treeInput)
      } else {
        null
      }

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.enableObjectReuse()

    val count_slide_percent: Double = 100 * (count_slide.toDouble / count_window)
    val time_window: Int = count_window / 10
    val time_slide: Int = (time_window * (count_slide_percent / 100)).toInt

    if (partitioning_type == "grid" && (algorithm == "pmcod" || algorithm == "advanced_vp" || algorithm == "slicing") && parallelism != 16) {
      System.exit(1)
    }

    val data = env.readTextFile(input)
    val mappedData = algorithm match {
      case "parallel" | "advanced" => data
        .flatMap(line => {
          val splitLine = line.split("&")
          val id = splitLine(0).toInt
          val value = splitLine(1).split(",").map(_.toDouble).to[ListBuffer]
          val multiplication = id / count_slide
          val new_time: Long = cur_time + (multiplication * time_slide)
          replicationPartitioning(parallelism, value, new_time, id)
        })
      case "pmcod" | "advanced_vp" | "slicing" => data
        .flatMap(line => {
          val splitLine = line.split("&")
          val id = splitLine(0).toInt
          val value = splitLine(1).split(",").map(_.toDouble).to[ListBuffer]
          val multiplication = id / count_slide
          val new_time: Long = cur_time + (multiplication * time_slide)
          if (partitioning_type == "grid")
            gridPartitioning(parallelism, value, new_time, id, range, dataset)
          else
            metricPartitioning(value, new_time, id, range, parallelism, "VPTree", null, myVPTree)
        })
    }

    val timestampData = mappedData
      .assignTimestampsAndWatermarks(new MyTimestamp)

    val myWindow = algorithm match {
      case "pmcod" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new Pmcod(time_slide, range, k))
      case "advanced_vp" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new AdvancedVP(time_slide, range, k))
      case "slicing" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new Slicing(time_slide, range, k))
      case "advanced" =>
        val firstWindow = timestampData
          .keyBy(_._1)
          .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
          .allowedLateness(Time.milliseconds(1000))
          .evictor(new MyEvictor(time_slide))
          .process(new Advanced(time_slide, range, k))
        firstWindow
          .keyBy(_.id % parallelism)
          .timeWindow(Time.milliseconds(time_slide))
          .process(new GroupMetadataAdvanced(time_window, time_slide, range, k))
      case "parallel" =>
        val firstWindow = timestampData
          .keyBy(_._1)
          .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
          .allowedLateness(Time.milliseconds(1000))
          .evictor(new MyEvictor(time_slide))
          .process(new Parallel(time_slide, range, k))
        firstWindow
          .keyBy(_.id % parallelism)
          .timeWindow(Time.milliseconds(time_slide))
          .process(new GroupMetadataParallel(time_window, time_slide, range, k))
    }

    val groupedOutliers = myWindow
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(time_slide))
      .process(new ShowOutliers)

    groupedOutliers.print()

    val time1 = System.currentTimeMillis()
    env.execute("Outlier-flink")
    val time2 = System.currentTimeMillis()
    println(s"My Time: ${time2 - time1}")

  }

}
