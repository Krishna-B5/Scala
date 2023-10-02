package pack

object Usecases23 {
  
  
  def main(args:Array[String]):Unit={
    
    println("\n======Original List=====")
    val data = List("Amazon-Jeff-America", 
                    "Microsoft-BillGates-America", 
                    "TCS-TATA-India", 
                    "Reliance-Ambani-India")
    data.foreach(println)
    
    println("\n=====Filter only India records=====")
    val data1 = data.filter(x => x.toLowerCase().contains("india"))
    data1.foreach(println )
    
    println("\n=====Flatten with Hyphen=====")
    val data2 = data1.flatMap(x => x.split("-"))
    data2.foreach(println)
    
    println("\n=====Replace India with Local=====")
    val data3 = data2.map(x => x.replace("India", "Local"))
    data3.foreach(println)
    
    println("\n=====Concatinate ,Done at the end=====")
    val data4 = data3.map(x => x.concat(",Done"))
    data4.foreach(println)
    
    
    println("\n===== Usecase2 ===== ")
    val lststr = List("State->TN~City->Chennai",
                      "State->Gujarat~City->GhandhiNagar"
                     )
    println("\n=====Orginal List=====")
    lststr.foreach(println)
    println("\n===== Flatten List=====")
    val data5 = lststr.flatMap(x => x.split("~"))
    val data6 = data5.flatMap(x => x.split("->"))
    data6.foreach(println)
        
  }
}