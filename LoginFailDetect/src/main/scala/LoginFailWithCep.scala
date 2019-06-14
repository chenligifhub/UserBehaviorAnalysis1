import org.apache.flink.cep.scala.{CEP, PatternStream, pattern}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
  * Created by cl on 2019/6/14.
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430832),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)

    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      //按照userId分流
      .keyBy(_.userId)

    //定义匹配模式，next紧邻发生的事件
    val loginEvent = Pattern
      //开始事件发生的类型fail
      .begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      //紧邻发生的类型也是fail
      .next("next")
      .where(_.eventType == "fail")
      //要求时间内发生事件
      .within(Time.seconds(2))
    //在keyby()之后的流中匹配出定义的事件
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginEvent)

    import scala.collection.Map
    //从pattenStream中获取事件流
    patternStream.select(
      (pattern: Map[String, Iterable[LoginEvent]]) => {
        //传参是上文String=next.("next")
        val next: LoginEvent = pattern.getOrElse("next", null).iterator.next()
        //返回数据结构
        (next.userId, next.ip, next.eventType)
      }
    )
      .print()
    env.execute("job")
  }
}
