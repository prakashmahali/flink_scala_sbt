package flink.batch

import org.apache.flink.api.scala._

object BatchProcessingJob {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // Static dataset
    val dataSet: DataSet[(Int, String, String)] = env.fromElements(
      (1, "Alice", "Engineer"),
      (2, "Bob", "Doctor"),
      (3, "Charlie", "Artist")
    )

    // Apply simple transformation
    val formattedData = dataSet.map(record => s"${record._2} is a ${record._3}")

    // Print results
    formattedData.print()
  }
}
