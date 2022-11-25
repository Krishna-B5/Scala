package pack

object obj1 {
  
  def main(args:Array[String])
  {
    println("List flatmap Operations")
    println()
    
    val list1 = List("Zeyo~Analytics","BigData~Spark","Hive~Sqoop")
    
    println("Original string :" +list1)
    println()
    
    val strflat = list1.flatMap( x => x.split("~") )
    
    println("Spearted string :"+strflat)
    println()
   
  }
}