package multi_rkws_param_outlier

import common_utils.Algorithms._
import common_utils.Partitioning._
import common_utils.Utils.find_gcd
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable.ListBuffer

case class MyNewQuery(R: Double, k: Int, W: Int, S: Int, var outliers: Int)

object OutlierDetect {

  //helper to slow down stream
  val cur_time = System.currentTimeMillis() + 1000000L //some delay for the correct timestamp

  def main(args: Array[String]) {
    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    //Parameters
    val input = parameters.getRequired("input")
    val treeInput = parameters.get("treeInput", "")
    val dataset = parameters.getRequired("dataset")
    val algorithm = parameters.getRequired("algorithm")
    val partitioning_type = parameters.get("part", "metric")
    val metric_count = parameters.get("VPcount", "10000").toInt
    val parallelism = parameters.getRequired("parallelism").toInt

    //Has to be distinct to remove useless computations
    val count_window_list = parameters.getRequired("window").split(",").toList.map(_.toInt)
    val count_slide_list = parameters.getRequired("slide").split(",").to[ListBuffer].map(_.toInt)
    val R_list = parameters.getRequired("r").split(",").toList.map(_.toDouble)
    val k_list = parameters.getRequired("k").split(",").toList.map(_.toInt)

    //Save queries
    val myQueries = ListBuffer[MyNewQuery]()
    for (i <- count_window_list.indices){
      val time_window: Int = count_window_list(i) / 10
      for (y <- count_slide_list.indices){
        val count_slide_percent: Double = 100 * (count_slide_list(y).toDouble / count_window_list(i).toInt)
        val time_slide: Int = (time_window * (count_slide_percent / 100)).toInt
        for(z <- R_list.indices){
          val myR = R_list(z)
          for(n <- k_list.indices){
            val myK = k_list(n)
            myQueries += new MyNewQuery(myR,myK,time_window,time_slide, 0)
          }
        }
      }
    }
    val range_max = R_list.max
    val count_window_max = count_window_list.max
    val count_slide_gcd = find_gcd(count_slide_list)
    val count_slide_percent: Double = 100 * (count_slide_gcd.toDouble / count_window_max)
    val time_window = count_window_max / 10
    val time_slide: Int = (time_window * (count_slide_percent / 100)).toInt

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

    if (partitioning_type != "metric") {
      System.exit(1)
    }

    if ((algorithm == "sop" || algorithm == "pmcsky" || algorithm == "psod") && parallelism != 16) {
      System.exit(1)
    }

    val data = env.readTextFile(input)
    val mappedData = data
      .flatMap(line => {
        val splitLine = line.split("&")
        val id = splitLine(0).toInt
        val value = splitLine(1).split(",").map(_.toDouble).to[ListBuffer]
        val multiplication = id / count_slide_gcd
        val new_time: Long = cur_time + (multiplication * time_slide)
        metricPartitioning(value, new_time, id, range_max, parallelism, "VPTree", null, myVPTree)
      })

    val timestampData = mappedData
      .assignTimestampsAndWatermarks(new MyTimestamp)

    val myWindow = algorithm match {
      case "pmcsky" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new PmcSky(time_slide, myQueries, cur_time))
      case "sop" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new Sop(time_slide, myQueries, cur_time))
      case "psod" => timestampData
        .keyBy(_._1)
        .timeWindow(Time.milliseconds(time_window), Time.milliseconds(time_slide))
        .allowedLateness(Time.milliseconds(1000))
        .process(new Psod(time_slide, myQueries, cur_time))
    }

    val groupedOutliers = myWindow
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(time_slide))
      .process(new GroupOutliers_MP)

    groupedOutliers.print()

    val time1 = System.currentTimeMillis()
    env.execute("Outlier-flink")
    val time2 = System.currentTimeMillis()
    println(s"My Time: ${time2 - time1}")

  }

}
