
import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Created by cl on 2019/6/11.
  */

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object ScalaHotItemsWithoutKafka {
  def main(args: Array[String]): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并发数
    env.setParallelism(1)
    import org.apache.flink.streaming.api.scala._
    val stream = env
      // .readTextFile("D:\\IntelliJ IDEA 2017.3.2\\workpace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      /**
        * 连接kafka数据源
        */
      .addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      .map(line => {
        val fileds: Array[String] = line.split(",")
        UserBehavior(fileds(0).toLong, fileds(1).toLong, fileds(2).toInt, fileds(3), fileds(4).toLong)
      })
      //指定时间戳和wotermark,此数据源是单调正序，可直接使用此函数
      .assignAscendingTimestamps(_.timestamp * 1000)
      //过滤，只需要pv类
      .filter(_.behavior == "pv")
      //按照itemId分流，根据itemId分到同一分区
      .keyBy("itemId")
      //设置窗口时间，和窗口流动时间
      .timeWindow(Time.hours(1), Time.minutes(5))

      /**
        * CountAgg()数据预先处理，聚合
        * WindowResultFunction()，封装数据
        */
      .aggregate(new CountAgg(), new WindowResultFunction())
      //根据windowEnd分流
      .keyBy("windowEnd")
      //取值
      .process(new TopNHotItem(3))
      .print()

    env.execute("job")
  }

  //自定义聚合函数
  class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def createAccumulator(): Long = 0L

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  //接收预聚合的数据=>in；处理数据，封装为ItemViewCount格式=>out；key必须是Tuple类型；
  class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count: Long = input.iterator.next()
      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

  //排序取值函数
  class TopNHotItem(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {
    /**
      * 存储接收到的每一条数据，存入ListState中
      */
    //定义状态ListState
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      //命名状态变量的名字和类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      //ItemViewCount聚合后的数据存入state中
      itemState.add(value)
      //注册定时器，触发时间定为windowEnd+1，证明已收集完数据
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    //定时器触发操作，从state中提取数据，排序输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //获取所有数据
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemState.get) {
        allItems += item
      }
      //释放空间
      itemState.clear()
      //按照点击量从大到小排序
      val sortItem = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      //打印格式
      val result = new StringBuilder
      result.append("====================================\n")
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
      //indices对象的index值
      for (elem <- sortItem.indices) {
        val currentItem: ItemViewCount = sortItem(elem)
        result.append("NO").append(elem + 1).append(":")
          .append("商品ID=").append(currentItem.itemId)
          .append("浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }

}
