package basic

object Exception extends App {
  val s1 : String = "123"
  val s2 : String = "123fff"
  val i,err = s1.toInt
  println(i)
  println(err)
  val i1= s1.toInt
  println(i1)
  val i2,err2 = s2.toInt
  println(i2)
  println(err2)
}
