package file_transforms

import org.apache.flink.api.scala.{ExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scala.io.Source
import java.io.File
import java.io.PrintWriter
import java.io._

object Add_id {
  def main(args: Array[String]) {

    val old_file1 = "DummyData/StockDatasets with ids/stock_1m.txt"
    val old_file2 = "DummyData/StockDatasets with ids/stock_10m.txt"
    val new_file = "DummyData/StockDatasets with ids/stock_10_10.txt"
    val lines = 191000

    reduce(old_file1,"DummyData/StockDatasets with ids/stock_10_20.txt",392000)
    reduce(old_file1,"DummyData/StockDatasets with ids/stock_10_50.txt",995000)
    reduce(old_file1,"DummyData/StockDatasets with ids/stock_10_1.txt",90100)
    reduce(old_file2,"DummyData/StockDatasets with ids/stock_100_10.txt",1910000)
    reduce(old_file2,"DummyData/StockDatasets with ids/stock_100_20.txt",3920000)
    reduce(old_file2,"DummyData/StockDatasets with ids/stock_100_50.txt",9950000)
    reduce(old_file2,"DummyData/StockDatasets with ids/stock_100_1.txt",901000)
    //add_id(old_file, new_file)
    //multiply(old_file, new_file)
    //get_stats(old_file)

  }


  def add_id(old_file: String, new_file: String): Unit ={
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)

    val data = env.readTextFile(old_file)
    var count = 0
    val ids = data.map(line => {
      val res = count.toString + "&" + line
      count += 1
      res
    })
    ids.writeAsText(new_file)


    env.execute("Outlier-flink")
  }

  def multiply(old_file: String, new_file: String): Unit ={

    val list = Source.fromFile(old_file).getLines().toList
    val listbig = list ::: list ::: list :::list ::: list ::: list:::list ::: list ::: list:::list


    val writer = new PrintWriter(new File(new_file))

    println(listbig.size)
    var count = 0
    listbig.foreach(l=> {
      writer.write(l + "\n")
      if(count%1000000 == 0) writer.flush()
    })

    writer.flush()
    writer.close()
  }

  def reduce(old_file: String, new_file: String, get: Int): Unit ={

    val list = Source.fromFile(old_file).getLines().toList

    val writer = new PrintWriter(new File(new_file))

    var count = 0
    list.take(get).foreach(l=> {
      writer.write(l + "\n")
      if(count%1000000 == 0) writer.flush()
    })

    writer.flush()
    writer.close()
  }

  def get_stats(old_file: String): Unit ={

    val data = Source.fromFile(old_file).getLines().toList.take(1000000)
    var min : Double = 10000
    var max : Double = -10000
    data.foreach(l=> {
      if (l.toDouble > max) max = l.toDouble
      if(l.toDouble < min ) min = l.toDouble
    })
    val normalized = data.map(l => {
      val value = l.toDouble
      //      val res = (value -min)/(max-min)
      value
    })

    val sorted = normalized.sorted

    val partitions = 32
    for (i <- 1 to partitions){
      val first = sorted.slice((i-1) * sorted.size/partitions, i * (sorted.size/partitions)).last
      println(first)
    }
  }
}