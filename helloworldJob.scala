package flink

import org.apache.flink.streaming.api.scala._

object HelloWorldJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Create a simple stream with "Hello" and "World"
    val dataStream: DataStream[String] = env.fromElements("Hello", "World")

    // Print the stream records
    dataStream.print()

    env.execute("Flink Hello World Streaming Job")
  }
}
