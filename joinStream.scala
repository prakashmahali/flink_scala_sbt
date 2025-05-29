package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.JoinFunction

object StreamJoinJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Create two streams with keys (ID, value)
    val stream1: DataStream[(Int, String)] = env.fromElements((1, "Alice"), (2, "Bob"))
    val stream2: DataStream[(Int, String)] = env.fromElements((1, "Engineer"), (2, "Doctor"))

    // Keyed stream join on ID
    val joinedStream: DataStream[String] = stream1
      .join(stream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
      .apply(new JoinFunction[(Int, String), (Int, String), String] {
        override def join(first: (Int, String), second: (Int, String)): String = {
          s"${first._2} is a ${second._2}"
        }
      })

    // Print the joined stream
    joinedStream.print()

    env.execute("Flink Stream Join Example")
  }
}
