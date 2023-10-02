package pack


object Usecases1 {
  
  def main(args:Array[String]):Unit={
    
    println("Video 23 Usecase 1")
    
    println("\nOriginal List")
    val listin = List("Zeyobron","Analytics","Zeyo")
    listin.foreach(println)
    
    println("\nIterate the each element and filter that contains Zeyo")
    val data1 = listin.filter( x => x.contains("Zeyo"))
    data1.foreach(println)
    
    println("\nIterate each element and concatinate ,bigdata")
    val data2 = listin.map(x => x.concat(",bigdata"))
    data2.foreach(println)
    
    println("\nIterate each element and replace Zeyo with Tera")
    val data3 = data2.map(x => x.replace("Zeyo", "Tera"))
    data3.foreach(println)
    
    println("\nThis is useccase for flatmap ")
    val list1 = List("Krishna~B","Kavitha~M","Takshvi~Krishna")
    val flatdata = list1.flatMap(x => x.split("~"))
    flatdata.foreach(println)
  }
}