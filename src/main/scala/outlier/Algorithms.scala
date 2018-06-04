package outlier

import java.lang
import Utils._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.evictors.Evictor
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object Algorithms {

  class MyTimestamp() extends AssignerWithPeriodicWatermarks[(Int, Data)] with Serializable {

    val maxOutOfOrderness = 1000L // 1 seconds

    override def extractTimestamp(e: (Int, Data), prevElementTimestamp: Long) = {
      val timestamp = e._2.arrival
      timestamp
    }

    override def getCurrentWatermark(): Watermark = {
      new Watermark(System.currentTimeMillis - maxOutOfOrderness)
    }
  }

  class MyEvictor(time_slide: Int) extends Evictor[(Int, Data), TimeWindow] {
    override def evictBefore(elements: lang.Iterable[TimestampedValue[(Int, Data)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
      val iteratorEl = elements.iterator
      while (iteratorEl.hasNext) {
        val tmpNode = iteratorEl.next().getValue._2
        if (tmpNode.flag == 1 && tmpNode.arrival >= window.getStart && tmpNode.arrival < window.getEnd - time_slide) iteratorEl.remove()
      }
    }

    override def evictAfter(elements: lang.Iterable[TimestampedValue[(Int, Data)]], size: Int, window: TimeWindow, evictorContext: Evictor.EvictorContext): Unit = {
    }
  }

  class ShowOutliers extends ProcessWindowFunction[(Long, Int), String, Long, TimeWindow] {
      override def process(key: Long, context: Context, elements: scala.Iterable[(Long, Int)], out: Collector[String]): Unit = {
        val outliers = elements.toList.map(_._2).sum
        out.collect(s"$key;$outliers")
      }
    }

  case class Metadata(var outliers: Map[Int, Data])

  class GroupMetadataAdvanced(time_window: Int, time_slide: Int, range: Double, k: Int) extends ProcessWindowFunction[Data, (Long, Int), Int, TimeWindow] {

    lazy val state: ValueState[Metadata] = getRuntimeContext
      .getState(new ValueStateDescriptor[Metadata]("metadata", classOf[Metadata]))

    override def process(key: Int, context: Context, elements: scala.Iterable[Data], out: Collector[(Long, Int)]): Unit = {
      val window = context.window
      var current: Metadata = state.value
      if (current == null) { //populate list for the first time
        var newMap = Map[Int, Data]()
        //all elements are new to the window so we have to combine the same ones
        //and add them to the map
        for (el <- elements) {
          val oldEl = newMap.getOrElse(el.id, null)
          if (oldEl == null) {
            newMap += ((el.id, el))
          } else {
            val newValue = combineElementsAdvanced(oldEl, el, k)
            newMap += ((el.id, newValue))
          }
        }
        //remove safe inliers
        var forRemoval = ListBuffer[Int]()
        for (el <- newMap) {
          if (el._2.count_after >= k) forRemoval = forRemoval.+=(el._2.id)
        }
        forRemoval.foreach(el => newMap -= (el))
        current = Metadata(newMap)
      } else { //update list

        //first remove old elements
        var forRemoval = ListBuffer[Int]()
        for (el <- current.outliers.values) {
          if (el.arrival < window.getEnd - time_window) {
            forRemoval = forRemoval.+=(el.id)
          }
        }
        forRemoval.foreach(el => current.outliers -= (el))
        //then insert or combine elements
        for (el <- elements) {
          val oldEl = current.outliers.getOrElse(el.id, null)
          if (oldEl == null && el.arrival >= window.getEnd - time_slide) { //insert new elements
            current.outliers += ((el.id, el))
          } else if (oldEl != null) {
            val newValue = combineElementsAdvanced(oldEl, el, k)
            current.outliers += ((el.id, newValue))
          }
        }

        //remove safe inliers
        forRemoval = ListBuffer[Int]()
        for (el <- current.outliers.values) {
          if (el.count_after >= k) forRemoval = forRemoval.+=(el.id)
        }
        forRemoval.foreach(el => current.outliers -= (el))
      }
      state.update(current)

      var outliers = ListBuffer[Int]()
      for (el <- current.outliers.values) {
        val nnBefore = el.nn_before.count(_ >= window.getEnd - time_window)
        if (nnBefore + el.count_after < k) outliers.+=(el.id)
      }

      out.collect((window.getEnd, outliers.size))
    }



  }

  class GroupMetadataParallel(time_window: Int, time_slide: Int, range: Double, k: Int) extends ProcessWindowFunction[(Data), (Long, Int), Int, TimeWindow] {

    lazy val state: ValueState[Metadata] = getRuntimeContext
      .getState(new ValueStateDescriptor[Metadata]("metadata", classOf[Metadata]))

    override def process(key: Int, context: Context, elements: scala.Iterable[Data], out: Collector[(Long, Int)]): Unit = {
      val time1 = System.currentTimeMillis()
      val window = context.window
      var current: Metadata = state.value
      if (current == null) { //populate list for the first time
        var newMap = Map[Int, Data]()
        //all elements are new to the window so we have to combine the same ones
        //and add them to the map
        for (el <- elements) {
          val oldEl = newMap.getOrElse(el.id, null)
          if (oldEl == null) {
            newMap += ((el.id, el))
          } else {
            val newValue = combineElementsParallel(oldEl, el, k)
            newMap += ((el.id, newValue))
          }
        }
        current = Metadata(newMap)
      } else { //update list

        //first remove old elements and elements that are safe inliers
        var forRemoval = ListBuffer[Int]()
        for (el <- current.outliers.values) {
          if (elements.count(_.id == el.id) == 0) {
            forRemoval = forRemoval.+=(el.id)
          }
        }
        forRemoval.foreach(el => current.outliers -= (el))
        //then insert or combine elements
        for (el <- elements) {
          val oldEl = current.outliers.getOrElse(el.id, null)
          if (oldEl == null) {
            current.outliers += ((el.id, el))
          } else {
            if (el.arrival < window.getEnd - time_slide) {
              oldEl.count_after = el.count_after
              current.outliers += ((el.id, oldEl))
            } else {
              val newValue = combineElementsParallel(oldEl, el, k)
              current.outliers += ((el.id, newValue))
            }
          }
        }
      }
      state.update(current)

      var outliers = ListBuffer[Int]()
      for (el <- current.outliers.values) {
        val nnBefore = el.nn_before.count(_ >= window.getEnd - time_window)
        if (nnBefore + el.count_after < k) outliers.+=(el.id)
      }
      out.collect((window.getEnd, outliers.size))
    }

  }

}
