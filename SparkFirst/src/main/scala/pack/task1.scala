package pack

object task1 {
  
  def main(args: Array[String]){
    
    println("===== List Operations =====")
    
    val liststr = List("Bigdata","spark","hive")
    val mapstr = liststr.map( x => "zeyo," + x)
    mapstr.foreach(println)
    
  }
}