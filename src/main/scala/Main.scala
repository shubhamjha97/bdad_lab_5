object Main {
  def loadData(path: String): org.apache.spark.rdd.RDD[(String, String)] = {
    sc.wholeTextFiles(path)
  }

  def flattenData(data: org.apache.spark.rdd.RDD[(String, String)]): org.apache.spark.rdd.RDD[String] = {
    data.map(x => x._2)
  }

  def parseData(data: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[(String, String)] = {
    data.map(scala.xml.XML.loadString).map(x => x \ "activation").flatMap(x=>x).map(x => (x \ "account-number", x \ "model")).map(x=>(x._1.text, x._2.text))
  }

  def formatData(data: org.apache.spark.rdd.RDD[(String, String)]): org.apache.spark.rdd.RDD[String] = {
    data.map(x => s"${x._1}:${x._2}")
  }

  def main: Unit = {
    // Load data
    val rawData = loadData("loudacre/activations")

    // Flatten data
    val flatData = flattenData(rawData)

    // Parse XML
    val parsedData = parseData(flatData)

    // Output Formatting
    val output = formatData(parsedData)

    // Write output to file
    output.saveAsTextFile("loudacre/account-models")
  }
}