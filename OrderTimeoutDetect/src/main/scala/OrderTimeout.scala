import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
  * Created by cl on 2019/6/14.
  */
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

case class OrderResult(orderId: Long, eventType: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val orderEventStream: KeyedStream[OrderEvent, Long] = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)
    //定义匹配事件窗口模式
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    //定义输出标签
    val orderTimeoutOutput: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")

    //订单事件流根据orderId分流,然后每一条流中匹配出定义好得模式
    val patternStream: PatternStream[OrderEvent] = CEP.pattern[OrderEvent](orderEventStream, orderPayPattern)

    import scala.collection.Map

    val complexResult: DataStream[OrderResult] = patternStream.select(orderTimeoutOutput) {
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val createOrderId: Long = pattern.getOrElse("begin", null).iterator.next().orderId
        OrderResult(createOrderId, "timeout")
      }
    } {
      //检测定好好的模式序列，就会调用这个函数
      pattern: Map[String, Iterable[OrderEvent]] => {
        val payOrderId: Long = pattern.getOrElse("follow", null).iterator.next().orderId
        OrderResult(payOrderId, "success")

      }
    }
    //拿到同一输出标签得timeout匹配结构（流）
    val timeoutResult: DataStream[OrderResult] = complexResult.getSideOutput(orderTimeoutOutput)

    complexResult.print()
    timeoutResult.print()
    env.execute("job")
  }
}
