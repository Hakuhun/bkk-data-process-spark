package hu.oe.bakonyi.bkk

object BkkDataProcesser {

  def add(x : Int, y : Int): Int ={
    return x + y;
  }

  def main(args: Array[String]): Unit = {
    print(s"Hello világ ${add(1,2)}")
  }


}
