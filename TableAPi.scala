package flink.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

object TableApiExample {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    // Create static dataset
    val data = env.fromElements(
      (1, "Alice", "Engineer"),
      (2, "Bob", "Doctor"),
      (3, "Charlie", "Artist")
    )

    // Convert to Table
    val table = tableEnv.fromDataSet(data, $"id", $"name", $"role")

    // Query the table
    val filteredTable = table.select($"name", $"role")

    // Convert Table back to DataSet and print results
    val result = tableEnv.toDataSet[(String, String)](filteredTable)
    result.print()
  }
}
