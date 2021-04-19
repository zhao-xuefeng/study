package test

object FastSort {
    def fastSort(a:List[Int]):  List[Int] ={
        a match {
            case Nil=>Nil
            case head :: tail =>
                val (left,right)=tail.partition(_ < head)
//                fastSort(left) ::: head :: fastSort(right)
                fastSort(left) ::: head :: fastSort(right)

        }
    }
    def quickSort(list: List[Int]): List[Int] = {
        list match {
            case Nil => Nil
            case List() => List()
            case head :: tail =>
                val (left, right) = tail.partition(_ < head)
                quickSort(left) ::: head :: quickSort(right)
        }
    }

    def main(args: Array[String]): Unit = {
        var list=List(1,8,3,2,6,9)
        print(fastSort(list))
    }

}
