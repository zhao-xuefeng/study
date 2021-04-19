package TrainFlinkStream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

case class Person(name:String,age:Int)
object TrainFlinkStream {

    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(12)//设置并行度
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      var dataStream=env.generateSequence(1,20)
      var d=dataStream.map(x=>x*2)
      d.filter(x=>x>12).print()
      env.execute()

    }
}
