import java.util.NoSuchElementException

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._

import scala.io.Source
import scala.util.matching.Regex

/**
  * Created by NK on 2017. 5. 22..
  */
object UnifyResult {

  val XBP = "<http://xb.saltlux.com/schema/property/>"

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val conf = new SparkConf()
      .setAppName("Unify Result")
      //      .setMaster("local[*]")
      .set("spark.io.compression.codec", "lz4")
      .set("spark.broadcast.compress", "true")
      .set("spark.locality.wait", "10000")
      .set("spark.shuffle.compress", "true")
      .set("spark.rdd.compress", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val resultPath = args(0)
    val queryPath = args(1)
    val triplePath = args(2)
    val unifiedResultPath = args(3)
    //    val resultPath = "query_result"
    //    val queryPath = "MJ-TestQuery/TQ4.rq"
    //    val triplePath = "nk,NK-TEST"
    //    val unifiedResultPath = "unified_result"
    val queryFile = Source.fromFile(queryPath)
    val queryString = Source.fromFile(queryPath).getLines().mkString(" ")
    val (variable, condition) = parseCondition(queryString)
    val resultsList = sc.textFile(resultPath).map(_.replace("(", "").replace(")", "").split(",").toList).filter(exceptionHandling1).collect().toList
    val triple = sc.textFile(triplePath).mapPartitions(parseNTriple, true).collect().toList

    printProgressTitle("=========== Query ===========")
    queryFile.getLines().foreach(println)

    printProgressTitle("=========== Query Result ===========")
    resultsList.foreach(println)

    printProgressTitle("=========== Query Variable ===========")
    variable.foreach(println)
    printProgressTitle("=========== Query Condition ===========")
    condition.foreach(println)

    val unifiedResultList: List[String] = unifyResult(condition, variable, resultsList)
    printProgressTitle("=========== Unified Triple ===========")
    unifiedResultList.foreach(println)
    printProgressTitle("=========== Filtering Triple ===========")
    val filteredTriple = filtering(unifiedResultList, triple).filter(exceptionHandling2).map { case (s, p, o) => List(s, p, o).mkString("") }
    filteredTriple.foreach(println)
    sc.parallelize(filteredTriple).coalesce(1).saveAsTextFile(unifiedResultPath)
    printProgressTitle("=========== Answer Explanation ===========")
    atms(sc, unifiedResultPath)
  }

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

  def exceptionHandling2(filteredTripleList: (String, String, String)): Boolean = {
    /**
      * reasoner를 통해 inferred 된 트리플 중
      * unify된 결과 중
      * person1 sibling person1
      * 과 같이 subject와 object가 같은 triple을 예외처리하기 위한 코드.
      */
    !(filteredTripleList._1 == filteredTripleList._3)
  }

  def parseCondition(queryString: String): (List[String], List[String]) = {
    val newQueryString = queryString.replace("\t", "")
    val reg = new Regex("SELECT (\\?.+) WHERE (\\{.+\\})")
    val variable = reg.findAllIn(newQueryString).matchData.next().group(1).split("\\,").map(_.trim).toList
    val condition = reg.findAllIn(newQueryString).matchData.next().group(2).replace("{", "").replace("}", "").replace("     ", "").replace("\t", "").trim.split(" \\.").map(_.trim).toList

    (variable, condition)
  }

  def fixChars(str: String): String = {
    val substitutions = Map(
      "{" -> "",
      "}" -> "",
      "     " -> ""
    )
    substitutions.foldLeft(str) { case (cur, (from, to)) => cur.replaceAll(from, to) }
  }

  def filtering(unifiedResult: List[String], triple: List[(String, String, String)]): List[(String, String, String)] = {
    val filteredTriple = scala.collection.mutable.ArrayBuffer.empty[List[(String, String, String)]]

    for (result <- unifiedResult) {
      val us = result.split(" ")(0)
      val up = result.split(" ")(1).split(":")(1)
      val uo = result.split(" ")(2)

      var mask = false

      filteredTriple += triple.filter { case (s, p, o) =>
        if (us.contains("?")) {
          mask = p.contains(up) && o.contains(uo)
        } else if (uo.contains("?")) {
          mask = s.contains(us) && p.contains(up)
        } else {
          mask = s.contains(us) && p.contains(up) && o.contains(uo)
        }
        mask
      }
    }
    filteredTriple.toList.flatten
  }

  def unifyResult(conditionList: List[String], variables: List[String], resultList: List[List[String]]) = {
    val unifiedTriple = scala.collection.mutable.ArrayBuffer.empty[String]

    for (result <- resultList) {
      for (condition <- conditionList) {
        var temp = condition
        for (i <- 0 to variables.length - 1) {
          temp = temp.replace(variables(i), result(i))
        }
        unifiedTriple += temp
      }
    }
    unifiedTriple.toList.distinct
  }

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

  def atms(sc: SparkContext, unifiedResultPath: String): Unit = {

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

    val unifiedResultList = sc.textFile(unifiedResultPath).collect().toList
    for (unifiedResult <- unifiedResultList) {
      val nodename = unifiedResult
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


        //println("-------------- Environments of Label ----------------")

        var count = 0
        printExp1("Query Result [" + unifiedResultList.indexOf(unifiedResult) +"] :")
        printExp3(nodename)
        for (env <- envs) {
          if (env.size != 1) {
            for (a <- env) {
              val triple = assumpTriples.get(a).get // URIs
              val output = "--" + triple
              println(output)
            }
            count += 1
          }
          println()
        }
      } catch {
        case e: Exception => {
          printExp1("Query Result [" + unifiedResultList.indexOf(unifiedResult) +"] :")
//          e.printStackTrace()
          printExp3(nodename)
          println()
        }
      }
      //      println("runtime : " + (System.currentTimeMillis() - time0) / 1000.0 + " sec")
    }

  }

  def explanation(): Unit = {

  }

  def printProgressTitle(str: String) = {
    println(Console.BOLD + Console.GREEN + str + Console.RESET)
  }

  def printExp1(str: String) = {
    println(Console.YELLOW + str + Console.RESET)
  }

  def printExp2(str: String) = {
    println(Console.MAGENTA + str + Console.RESET)
  }

  def printExp3(str: String) = {
    println(Console.CYAN + str + Console.RESET)
  }

}
