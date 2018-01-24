package file_transforms

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala.createTypeInformation

object Add_id {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val data = env.readTextFile("DummyData/stock.txt")
    var count = 0
    val ids = data.map(line => {
      val res = count.toString + "&" + line
      count += 1
      res
    })
    ids.writeAsText("stock_id.txt")


    env.execute("Outlier-flink")
  }
}
