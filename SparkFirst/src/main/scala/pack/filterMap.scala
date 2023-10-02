package pack

object filterMap {
  
	def main(args: Array[String]):Unit={
	  
		println("Hello, world!")

		val listin = List(1,2,3,4)
		println(listin)
		listin.foreach(println)
		
		println("Integer")
		val a = 2;
		println(a)
		
		println("String")
		val b = "Krishna"
		println(b)

		val data = listin.filter( a => a == 2)
		data.foreach(println)

		val data1 = listin.map(x => (x * 100) / 2)
		println(data1)

		val data2 = listin.map(x => x + 100)
		println(data2)
	}
}