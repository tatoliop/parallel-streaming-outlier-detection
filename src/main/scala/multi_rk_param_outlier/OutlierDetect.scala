package multi_rk_param_outlier

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import common_utils.Algorithms._
import common_utils.Partitioning._

import scala.collection.mutable.ListBuffer

case class MyQuery(R: Double, k: Int, var outliers: Int)

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
    val partitioning_type = parameters.get("part", "metric")
    val metric_count = parameters.get("VPcount", "10000").toInt
    val parallelism = parameters.getRequired("parallelism").toInt
    val queries = parameters.getRequired("q").split(",").toList
      .map(p => {
        val split = p.trim.split(";")
        (split(0).toInt, split(1).toDouble)
      })
    val range_max = queries.map(_._2).max

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

    if (partitioning_type != "metric") {
      System.exit(1)
    }
    if ((algorithm == "pamcod" || algorithm == "sop" || algorithm == "pmcsky" || algorithm == "psod") && parallelism != 16) {
      System.exit(1)
    }

    val data = env.readTextFile(input)
    val mappedData = data
      .flatMap(line => {
        val splitLine = line.split("&")
        val id = splitLine(0).toInt
        val value = splitLine(1).split(",").map(_.toDouble).to[ListBuffer]
        val multiplication = id / count_slide
        val new_time: Long = cur_time + (multiplication * time_slide)
        metricPartitioning(value, new_time, id, range_max, parallelism, "VPTree", null, myVPTree)
      })

    val timestampData = mappedData
      .assignTimestampsAndWatermarks(new MyTimestamp)

    val myWindow = algorithm match {
      case "pamcod" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new Pamcod(time_slide, queries))
      case "pmcsky" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new PmcSky(time_slide, queries))
      case "sop" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new Sop(time_slide, queries))
      case "psod" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new Psod(time_slide, queries))
    }

    val groupedOutliers = myWindow
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(time_slide))
      .process(new GroupOutliers)

    groupedOutliers.print()

    val time1 = System.currentTimeMillis()
    env.execute("Outlier-flink")
    val time2 = System.currentTimeMillis()
    println(s"My Time: ${time2 - time1}")

  }

}
