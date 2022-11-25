package pack

import org.apache.spark._

object obj {
  
  def main(args:Array[String]):Unit={
    
    // println("Hello Zeyo")
    
    val liststr = List("Zeyoborn","Analytics","Zeyo")
    
//   val str = liststr.filter(x => x.contains("Zeyo"))
    
//   str.foreach(println)
   
//   val strcon = liststr.map( x => x.concat(", BigData") )
   
//   println("Concanted string: "+strcon)
    
    val strrep = liststr.map( x => x.replace("Zeyo", "Tera") )
    println("Original string :"+liststr)   
    println()
    println("Replaced string :"+strrep)
        
  }
}