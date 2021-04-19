package flinkWord

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

object FlinkWord {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //加载数据源
//    var source =env.readTextFile("person")
    val source = env.fromElements("china is the best country","beijing is the capital of china")
    //转化处理数据
    val ds = source.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    //输出至目的端
    ds.print()
    // 执行操作
    // 由于是Batch操作，当DataSet调用print方法时，源码内部已经调用Excute方法，所以此处不再调用，如果调用会出现错误
//    env.execute("Flink Batch Word Count By Scala")
  }


}
