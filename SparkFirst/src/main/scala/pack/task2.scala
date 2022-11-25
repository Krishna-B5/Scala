package pack

object task2 {
  
    def main(args:Array[String]):Unit={
      
      val liststr1 = List("State->TN~City->Chennai" ,"State->Gujarat~City->GandhiNagar")
      println("===== Orogonal List =====")
      liststr1.foreach(println)
      println
      println("===== Flatten 1 =====")
      val flatmap1= liststr1.flatMap( x => x.split("~"))
      flatmap1.foreach(println)
      println
      println("===== Flatten 2 =====")
      val flatmap2= flatmap1.flatMap( x => x.split("->"))
      flatmap2.foreach(println)
      println()
    
  }
}