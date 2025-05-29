package flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.RichCoFlatMapFunction
import org.apache.flink.util.Collector

object SimpleStreamJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Create two keyed streams
    val stream1: KeyedStream[(Int, String), Int] = env.fromElements((1, "Alice"), (2, "Bob")).keyBy(_._1)
    val stream2: KeyedStream[(Int, String), Int] = env.fromElements((1, "Engineer"), (2, "Doctor")).keyBy(_._1)

    // Connect the streams
    val joinedStream: DataStream[String] = stream1
      .connect(stream2)
      .flatMap(new RichCoFlatMapFunction[(Int, String), (Int, String), String] {
        var map1: Map[Int, String] = Map()
        var map2: Map[Int, String] = Map()

        override def flatMap1(value: (Int, String), out: Collector[String]): Unit = {
          map1 += (value._1 -> value._2)
          if (map2.contains(value._1)) {
            out.collect(s"${value._2} is a ${map2(value._1)}")
          }
        }

        override def flatMap2(value: (Int, String), out: Collector[String]): Unit = {
          map2 += (value._1 -> value._2)
          if (map1.contains(value._1)) {
            out.collect(s"${map1(value._1)} is a ${value._2}")
          }
        }
      })

    // Print the joined stream
    joinedStream.print()

    env.execute("Flink Non-Windowed Stream Join Example")
  }
}
