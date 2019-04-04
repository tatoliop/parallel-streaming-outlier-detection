package file_transforms

import java.io.{File, PrintWriter}

import common_utils.Data
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

//import breeze.linalg._
//import breeze.plot._
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import common_utils.Algorithms._
import outlier._
import outlier.OutlierDetect.cur_time

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Add_id {


  def main(args: Array[String]): Unit = {

    val S_distinct_list = ListBuffer[Int](2,3,5)
    val S_max = S_distinct_list.max
    val S_max_2 = S_distinct_list.filter(_ != S_max).max
    val S_var = List.range(1, S_distinct_list.product + 1).filter(p => {
      var done = false
      S_distinct_list.foreach(l => {
        if(p%l == 0) done = true
        })
      done
    }).distinct.sorted
    println(S_var)

  }

  def gcd(a: Int, b: Int): Int= {
    if(a==0) return b
    gcd(b%a,a)
  }

  def find_gcd(myList: ListBuffer[Int]): Int = {
    var result = myList(0)
    for (i <- 1 until myList.size){
      result = gcd(myList(i), result)
    }
    result
  }

  def add_id(old_file: String, new_file: String): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val data = env.readTextFile(old_file)

    var count = 0
    data.map(r => {
      val toWrite = count + "&" + r
      count += 1
      toWrite
    }).writeAsText(new_file).setParallelism(1)

    env.execute("Outlier-flink")
  }

  def multiply(old_file: String, new_file: String): Unit = {

    val list = Source.fromFile(old_file).getLines().toList
    val listbig = list ::: list ::: list ::: list ::: list ::: list ::: list ::: list ::: list ::: list :::
      list ::: list ::: list ::: list ::: list ::: list ::: list ::: list ::: list ::: list


    val writer = new PrintWriter(new File(new_file))

    var count = 0
    listbig.foreach(l => {
      writer.write(l + "\n")
      if (count % 1000000 == 0) writer.flush()
    })

    writer.flush()
    writer.close()
  }

  def reduceDim(old_file: String, new_file: String): Unit = {

    val list = Source.fromFile(old_file).getLines().toList

    val writer = new PrintWriter(new File(new_file))

    list.foreach(l => {
      val toWriteTemp = l.split(",")
      val toWrite = toWriteTemp(1) + "," + toWriteTemp(4)
      writer.write(toWrite + "\n")
    })

    writer.flush()
    writer.close()
  }

  def reduce(old_file: String, new_file: String, get: Int): Unit = {

    val list = Source.fromFile(old_file).getLines().toList

    val writer = new PrintWriter(new File(new_file))

    list.take(get).foreach(l => {
      writer.write(l + "\n")
    })

    writer.flush()
    writer.close()
  }

  def get_stats(old_file: String): Unit = {

    val data = Source.fromFile(old_file).getLines().toList
    val data1d = data.map(_.split(",")(0).toDouble)
    val data2d = data.map(_.split(",")(1).toDouble)
    //    val data3d = data.map(_.split(",")(2).toDouble)
    //    val data4d = data.map(_.split(",")(3).toDouble)
    //    val data5d = data.map(_.split(",")(4).toDouble)
    val sorted1d = data1d.sorted
    val sorted2d = data2d.sorted
    //    val sorted3d = data3d.sorted
    //    val sorted4d = data4d.sorted
    //    val sorted5d = data5d.sorted

    var partitions = 4
    for (i <- 1 until partitions) {
      val first = sorted1d.slice((i - 1) * sorted1d.size / partitions, i * (sorted1d.size / partitions)).last
      println(s"1d: $first")
    }
    partitions = 4
    for (i <- 1 until partitions) {
      val first = sorted2d.slice((i - 1) * sorted2d.size / partitions, i * (sorted2d.size / partitions)).last
      println(s"2d: $first")
    }
    //    partitions = 2
    //    for (i <- 1 until partitions){
    //      val first = sorted3d.slice((i-1) * sorted3d.size/partitions, i * (sorted3d.size/partitions)).last
    //      println(s"3d: $first")
    //    }
    //    partitions = 2
    //    for (i <- 1 until partitions){
    //      val first = sorted4d.slice((i-1) * sorted4d.size/partitions, i * (sorted4d.size/partitions)).last
    //      println(s"4d: $first")
    //    }
    //    partitions = 2
    //    for (i <- 1 until partitions){
    //      val first = sorted5d.slice((i-1) * sorted5d.size/partitions, i * (sorted5d.size/partitions)).last
    //      println(s"5d: $first")
    //    }
  }

//  def plot(file: String, dimension: Int): Unit = {
//    val input1 = Source.fromFile(file).getLines().map(_.split(",")(dimension).toDouble).toSeq
//    val input2 = input1.groupBy(p => p).mapValues(_.size)
//    val map: Seq[(Double, Int)] = input2.toSeq.sortBy(_._1)
//
//    val dim1 = DenseVector(map.map(_._1): _*)
//    val dim2 = DenseVector(map.map(_._2.toDouble): _*)
//
//    val fig = Figure()
//    var plt = fig.subplot(0)
//    plt += breeze.plot.scatter(dim1, dim2, {
//      (_: Int) => 0.1
//    })
//    fig.refresh()
//  }

  def normalize(file: String, newFile: String): Unit = {
    val data = Source.fromFile(file).getLines().toList
      .map(r => (r.split(",")(0).toDouble, r.split(",")(1).toDouble
        , r.split(",")(2).toDouble, r.split(",")(3).toDouble, r.split(",")(4).toDouble))

    val dim1min = data.map(_._1).min
    val dim1max = data.map(_._1).max
    val dim2min = data.map(_._2).min
    val dim2max = data.map(_._2).max
    val dim3min = data.map(_._3).min
    val dim3max = data.map(_._3).max
    val dim4min = data.map(_._4).min
    val dim4max = data.map(_._4).max
    val dim5min = data.map(_._5).min
    val dim5max = data.map(_._5).max

    val dataNorm = data.map(r => ((r._1 - dim1min) / (dim1max - dim1min), (r._2 - dim2min) / (dim2max - dim2min),
      (r._3 - dim3min) / (dim3max - dim3min), (r._4 - dim4min) / (dim4max - dim4min),
      (r._5 - dim5min) / (dim5max - dim5min)))

    val writer = new PrintWriter(new File(newFile))
    var count = 0
    dataNorm.foreach(r => {
      val toWrite = f"${
        r._1
      }%1.5f" + "," + f"${
        r._2
      }%1.5f" + "," + f"${
        r._3
      }%1.5f" +
        "," + f"${
        r._4
      }%1.5f" + "," + f"${
        r._5
      }%1.5f" + "\n"
      writer.write(toWrite)
      if (count % 10000 == 0) {
        writer.flush()
        println(count)
      }
      count += 1
    })
    writer.flush()
    writer.close()
  }

  def skybandTest(): Unit = {

    val listC: List[(Int, Int)] = List((1, 1), (10, 2), (10, 5), (10, 1), (10, 1), (11, 2), (11, 3), (1, 5), (2, 2), (2, 3), (2, 2), (3, 4), (4, 5), (5, 1), (6, 2), (6, 5),
      (7, 4), (8, 3), (9, 4), (9, 2))
    val list = listC.sortBy(_._1)
    println(list)
    val k = 10
    val res: ListBuffer[(Int, Int)] = ListBuffer()

    for (i <- 0 until list.size) {
      var counter = 0
      for (y <- 0 until i) {
        if (list(y)._2 > list(i)._2) counter += 1
      }
      if (counter <= k) {
        res.append(list(i))
      }
    }

    res.foreach(println)
  }

}