import java.util.NoSuchElementException

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.matching.Regex

/**
  * Created by NK on 2017. 5. 22..
  */
object UnifyResult {
  val conditionBodyMap = scala.collection.mutable.HashMap.empty[String, ArrayBuffer[String]]
  val resultMap = scala.collection.mutable.HashMap.empty[String, ArrayBuffer[List[String]]]
  val orderList = scala.collection.mutable.ArrayBuffer.empty[String]

  val prefixMap = Map[String, String](
    "rdf" -> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#%s>",
    "rdfs" -> "<http://www.w3.org/2000/01/rdf-schema#%s>",
    "owl" -> "<http://www.w3.org/2002/07/owl#%s>",
    "ub" -> "<http://ssu.ac.kr#%s>",
    "xbp" -> "<http://xb.saltlux.com/schema/property/%s>",
    "xsd" -> "<http://www.w3.org/2001/XMLSchema#%s>",
    "xbr" -> "<http://xb.saltlux.com/resource/%s>",
    "xbc" -> "<http://xb.saltlux.com/schema/class/%s>",
    "xbv" -> "<http://xb.saltlux.com/schema/vocab/%s>"
  )
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf()
      .setAppName("Unify Result")
      .setMaster("local[*]")
      .set("spark.io.compression.codec", "lz4")
      .set("spark.broadcast.compress", "true")
      .set("spark.locality.wait", "10000")
      .set("spark.shuffle.compress", "true")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

//    val resultPath = args(0)      // Sparqlgx Query 결과. (HDFS)
//    val queryPath = args(3)       // sparqlgx 돌린 Query File
//    val tempResultPath = args(1)  // Sparqlgx Temp Query모든 Variable이 들어간) 결과. (HDFS)
//    val tmpQueryPath = args(2)    // Sparqlgx 돌린 Temp Query File

    val resultPath = "query_result"
    val queryPath = "query/TQ1.rq"
    val tmpQueryPath = "query/tmp/TQ1.rq"
    val tempResultPath = "tmp_query_result"


    val queryFile = Source.fromFile(queryPath)
    val tmpQueryString = Source.fromFile(tmpQueryPath).getLines().mkString(" ")
    val (tmpVariableList, tmpConditionList) = parseCondition(tmpQueryString)
    val resultsList = sc.textFile(resultPath).map(makeFormForAnswerResult).collect().toList
    val tmpResultsList = sc.textFile(tempResultPath).map(makeFormForAnswerResult).collect().toList

    val unifiedTemporaryResultMap: mutable.HashMap[String, ArrayBuffer[String]] = unifyingResult(tmpConditionList, tmpVariableList, tmpResultsList)
    val unifiedAnswerTripleMap = generateUnifiedAnswerMap(resultsList, unifiedTemporaryResultMap)

    printProgressTitle("=========== Query ===========")
    queryFile.getLines().foreach(println)

    //    printProgressTitle("=========== Query Result ===========")
    //    resultsList.foreach(println)

    printProgressTitle("=========== Answer Explanation ===========")
//    val time0 = System.currentTimeMillis()
    atms(sc, resultsList, unifiedAnswerTripleMap)
//    val time1 = System.currentTimeMillis()
//    val lapse = (time1 - time0)/unifiedAnswerTripleMap.size
//    println("Time : " + lapse/1000 + "." + lapse%1000 + "sec")
  }

  /**
    *
    * @param resultList
    * @param unifiedTempAnswerTripleMap
    * @return
    */
  def generateUnifiedAnswerMap(resultList:List[String], unifiedTempAnswerTripleMap:mutable.HashMap[String, ArrayBuffer[String]]): mutable.HashMap[String, ArrayBuffer[List[String]]] ={
    val unifiedAnswerTripleMap = scala.collection.mutable.HashMap.empty[String, ArrayBuffer[List[String]]]

    // rst1 = (A1, B1)
    for(rst1 <- resultList){
      // rst2 = (A1, B1, C1, D1)
      for((rst2, triples) <- unifiedTempAnswerTripleMap){
        if(rst2.contains(rst1)){
          var triplesList:ArrayBuffer[List[String]] = null
          if(unifiedAnswerTripleMap.contains(rst1)){
            triplesList = unifiedAnswerTripleMap(rst1)
          }else{
            triplesList = scala.collection.mutable.ArrayBuffer.empty[List[String]]
          }
          triplesList += triples.toList
          unifiedAnswerTripleMap(rst1) = triplesList
        }
      }
    }
    unifiedAnswerTripleMap
  }

  /**
    *
    * @param tripleBySeparatedSpace
    * @return
    */
  def makeFormForAtmsWhyInput(tripleBySeparatedSpace:String):String = {
    tripleBySeparatedSpace.split(" ").mkString("")
  }

  /**
    *
    * @param hdfsOutputStr
    * @return
    */
  def makeFormForAnswerResult(hdfsOutputStr:String): String = {
    hdfsOutputStr.replace("(", "").replace(")", "")
  }

  /**
    *
    * @param str
    * @return
    */
  def makeFormForPredicateTerm(str:String): String = {
    val (prefix, term) = (str.split(":")(0), str.split(":")(1))
    val fullURIFormat = prefixMap(prefix)
    fullURIFormat.format(term)
//    "<http://xb.saltlux.com/schema/property/"+str+">"
  }

  /**
    *
    * @param t
    * @return
    */
  def makeFormForTriple(t:String): (String, String, String) = {
    val s = t.split(" ")(0)
    val p = t.split(" ")(1)
    var o = t.split(" ")(2)

    val (prefix, term) = (o.split(":")(0), o.split(":")(1))
    if(prefixMap.contains(prefix)){
      val fullURIString = prefixMap(prefix)
      o = fullURIString.format(term)
    }

    (s, makeFormForPredicateTerm(p), o)
  }

  def removeURI(term:String): String = {
    term.replace("http://xb.saltlux.com/schema/property/", "").replace("http://xb.saltlux.com/resource/","")
  }

  /**
    * reasoner를 통해 inferred 된 트리플 중
    * person1 spouse person1
    * 위와 같은 경우에 대한 예외처리.
    *
    * @param resultList
    * @return
    */
  def exceptionHandling1(resultList: List[String]): Boolean = {
    /**
      * reasoner를 통해 inferred 된 트리플 중
      * person1 spouse person1
      * 위와 같은 경우에 대한 예외처리.
      */
    if (resultList.size == 2) {
      !(resultList(0) == resultList(1))
    } else {
      true
    }
  }

  /**
    * reasoner를 통해 inferred 된 트리플 중
    * unify된 결과 중
    * person1 sibling person1
    * 과 같이 subject와 object가 같은 triple을 예외처리하기 위한 코드.
    *
    * @param filteredTripleList
    * @return
    */
  def exceptionHandling2(filteredTripleList: (String, String, String)): Boolean = {
    !(filteredTripleList._1 == filteredTripleList._3)
  }

  /**
    *
    * @param queryString
    * @return
    */
  def parseCondition(queryString: String): (List[String], List[String]) = {
    val newQueryString = queryString.replace("\t", "")
    val reg = new Regex("SELECT\\s+(\\?.+)\\s+WHERE.+(\\{.+\\})")
    val variable = reg.findAllIn(newQueryString).matchData.next().group(1).split("\\,").map(_.trim).toList
    val condition = reg.findAllIn(newQueryString).matchData.next().group(2).replace("{", "").replace("}", "").replace("     ", "").replace("\t", "").trim.split(" \\.").map(_.trim).toList

    (variable, condition)
  }

  /**
    *
    * @param str
    * @return
    */
  def fixChars(str: String): String = {
    val substitutions = Map(
      "{" -> "",
      "}" -> "",
      "     " -> ""
    )
    substitutions.foldLeft(str) { case (cur, (from, to)) => cur.replaceAll(from, to) }
  }

  /**
    *
    * @param conditionList
    * @param variables
    * @param resultList
    * @return
    */
  def unifyingResult(conditionList: List[String], variables: List[String], resultList: List[String]):mutable.HashMap[String, ArrayBuffer[String]] = {
    var unifiedTriple:ArrayBuffer[String] = null//scala.collection.mutable.ArrayBuffer.empty[String]
    val unifiedMap = scala.collection.mutable.HashMap.empty[String, ArrayBuffer[String]]

    for(rst <- resultList){
      if(unifiedMap.contains(rst)){
        unifiedTriple = unifiedMap(rst)
      }else{
        unifiedTriple = scala.collection.mutable.ArrayBuffer.empty[String]
      }

      for (condition <- conditionList) {
        var temp = condition
        val tmpResult = rst.split(",")
        for (i <- variables.indices) {
          temp = temp.replace(variables(i), tmpResult(i))
        }
        val (s, p, o) = makeFormForTriple(temp)
        unifiedTriple += List(s, p, o).mkString(" ")
      }
      unifiedMap(rst) = unifiedTriple
    }

    unifiedMap
  }

  /**
    *
    * @param lines
    * @return
    */
  def parseNTriple(lines: Iterator[String]) = {
    val TripleParser = new Regex("(<[^\\s]*>)|(_:[^\\s]*)|(\".*\")")
    for (line <- lines) yield {
      try {
        val tokens = TripleParser.findAllIn(line)
        val (s, p, o) = (tokens.next(), tokens.next(), tokens.next())
        (s, p, o)

      } catch {
        case nse: NoSuchElementException => {
          ("ERROR", "ERROR", "ERROR")
        }
      }
    }
  }

  /**
    *
    * @param sc
    * @param queryResultList
    * @param unifiedTripleMap
    */
  def atms(sc: SparkContext, queryResultList: List[String],
           unifiedTripleMap:mutable.HashMap[String, ArrayBuffer[List[String]]]): Unit = {

    var indexedHolds: IndexedRDD[String, Set[Set[Int]]] = null
    var indexedAssumptions: IndexedRDD[Long, String] = null
    var indexedJustificands: IndexedRDD[Long, (String, List[String])] = null
    var indexedEnvs: IndexedRDD[String, Long] = null

    var indexedNodes: IndexedRDD[String, Long] = null

    var indexedJustifiers: IndexedRDD[String, Iterable[Long]] = null
    var indexedJustifieds: IndexedRDD[String, Iterable[Long]] = null
    var nogoodListInt: Iterable[List[Int]] = Iterable.empty

    indexedHolds = IndexedRDD(sc.objectFile("output/holds"))
    indexedHolds.count()
    indexedAssumptions = IndexedRDD(sc.objectFile("output/assumptions"))
    indexedAssumptions.count()

    indexedJustificands = IndexedRDD(sc.objectFile("output/justs"))
    indexedJustificands.count()
    indexedEnvs = IndexedRDD(sc.objectFile("output/envs"))
    indexedEnvs.count()
    indexedJustifiers = IndexedRDD(sc.objectFile("output/justifiers"))
    indexedJustifiers.count()
    indexedJustifieds = IndexedRDD(sc.objectFile("output/justifieds"))
    indexedNodes = IndexedRDD(sc.objectFile("output/indexedNodes"))

    for((answer, triplesList) <- unifiedTripleMap){
      printExp1("Explanation of Query Answer[ " + removeURI(answer)+ " ]")
      for (triples <- triplesList) {
        for(t <- triples){
          val nodename = makeFormForAtmsWhyInput(t)
          val time0 = System.currentTimeMillis()

          try {
            val envs: Set[Set[Int]] = indexedHolds.get(nodename).get

            val localEnvIndices: Map[String, Long] = indexedEnvs.multiget(envs.map(e => e.mkString(",")).toArray)

            // assumId
            val assumpOfEnv: Array[Long] = envs.flatMap(e => e.map(a => a.toLong)).toArray
            // assumId, assumTriple
            val assumpTriples: Map[Int, String] =
              indexedAssumptions.multiget(assumpOfEnv)
                .map { case (assumId, assumTriple) => (assumId.toInt, assumTriple) }

            var count = 0
            printExp3(removeURI(nodename).replace("><", "> <"))
            for (env <- envs) {
              if (env.size != 1) {
                for (a <- env) {
                  val triple = assumpTriples(a) // URIs
                  val output = "--> " + removeURI(triple).replace("><", "> <")
                  println(output)
                }
                count += 1
              }
              println()
            }
          } catch {
            case e: Exception => {
              //            printExp1("Query Result [" + unifiedResultList.indexOf(triple) +"] :")
              //          e.printStackTrace()
              printExp3(removeURI(nodename).replace("><", "> <"))
              println()
            }
          }
          //      println("runtime : " + (System.currentTimeMillis() - time0) / 1000.0 + " sec")
        }
      }
    }
  }

  /**
    *
    * @param str
    */
  def printProgressTitle(str: String) = {
    println(Console.BOLD + Console.GREEN + str + Console.RESET)
  }

  /**
    *
    * @param str
    */
  def printExp1(str: String) = {
    println(Console.YELLOW + str + Console.RESET)
  }

  /**
    *
    * @param str
    */
  def printExp2(str: String) = {
    println(Console.MAGENTA + str + Console.RESET)
  }

  /**
    *
    * @param str
    */
  def printExp3(str: String) = {
    println(Console.CYAN + str + Console.RESET)
  }

}
