package com.buildupchao.flinkexamples.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

/**
  * @author buildupchao
  * @date 2020/01/04 19:12
  * @since JDK 1.8
  */
object WordCountScalaExample {

  def main(args: Array[String]): Unit = {
    // 1.设定执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2.指定数据源地址，读取输入数据
    val filePath = System.getProperty("user.dir") + "/data/wordcount.txt"
    val text = env.readTextFile("file://" + filePath)
    // 3.对数据集指定转换操作逻辑
    val counts: DataStream[(String, Int)] =
      text.flatMap(_.toLowerCase.split("\\s+"))
        .filter(_.nonEmpty)
        .map((_, 1))
        .keyBy(0)
        .sum(1)

    // 4.指定计算结果输出位置
     val params = ParameterTool.fromArgs(args)
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // 5.指定名称并触发流式任务
    env.execute("Streaming WordCount")
  }
}
