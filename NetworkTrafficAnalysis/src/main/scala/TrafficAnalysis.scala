import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Created by cl on 2019/6/12.
  */

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object TrafficAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
      .readTextFile("D:\\IntelliJ IDEA 2017.3.2\\workpace\\UserBehaviorAnalysis\\NetworkTrafficAnalysis\\src\\main\\resources\\apachetest.log")
      .map(line => {
        val arr: Array[String] = line.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyy:HH:mm:ss")
        val timestamp: Long = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(2), timestamp, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent]
      (Time.seconds(10)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
        }
      })
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
      .print()
    env.execute("job")
  }

  class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def createAccumulator(): Long = 0L

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFunction extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url: String = key
      val count: Long = input.iterator.next()
      out.collect(UrlViewCount(url, window.getEnd, count))
    }
  }

  class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState", classOf[UrlViewCount]))

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urlState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 10 * 1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (urlView <- urlState.get()) {
        allUrlViews += urlView
      }
      urlState.clear()
      val sortUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      val result: StringBuilder = new StringBuilder
      result.append("=========================================\n")
      result.append("时间:").append(new Timestamp(timestamp - 10 * 1000)).append("\n")
      for (elem <- sortUrlViews.indices) {

        val currentUrlView: UrlViewCount = sortUrlViews(elem)
        result.append("No").append(elem + 1).append(":")
          .append("URL=").append(currentUrlView.url)
          .append("流量=").append(currentUrlView.count).append("\n")
      }
      result.append("============================================\n\n")
      Thread.sleep(1000)
      out.collect(result.toString())
    }
  }

}
