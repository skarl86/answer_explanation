import java.util.NoSuchElementException

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

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

    val conf = new SparkConf().setAppName("Unify Result").setMaster("local[*]")
    val sc = new SparkContext(conf)

//    val resultPath = args(0)
//    val queryPath = args(1)
//    val triplePath = args(2)
    val resultPath = "result"
    val queryPath = "MJ-TestQuery/TQ1.rq"
    val triplePath = "triple"

    val queryString = Source.fromFile(queryPath).getLines().mkString(" ")
    val (variable, condition) =  parseCondition(queryString)
    val resultsList= sc.textFile(resultPath).map(_.replace("(","").replace(")","").split(",").toList).collect().toList
    val triple = sc.textFile(triplePath).mapPartitions(parseNTriple, true).collect().toList

    /////////////////////////////////////
    /////////////////////////////////////




    resultsList.foreach(println)

    println(variable)
    println(condition)

    val unifiedResultList:List[String] = unifyResult(condition, variable, resultsList)
    unifiedResultList.foreach(println)

    val a = List("a","b","c")
    val b = List("a","b")

    println(a.contains(b))
    println(b.contains(a))
    filtering(unifiedResultList, triple).foreach(println)

    triple.filter{case (s, p, o) => o.contains("")}
  }
  def parseCondition(queryString:String):(List[String], List[String]) = {
    val newQueryString = queryString.replace("\t", "")
    val reg = new Regex("SELECT (\\?.+) WHERE (\\{.+\\})")
    val variable = reg.findAllIn(newQueryString).matchData.next().group(1).split("\\,").map(_.trim).toList
    val condition = reg.findAllIn(newQueryString).matchData.next().group(2).replace("{", "").replace("}", "").replace("     ", "").trim.split(" \\.").toList

    (variable, condition)
  }

  def fixChars(str: String): String = {
    val substitutions = Map(
      "{" -> "",
      "}" -> "",
      "     " -> ""
    )

    substitutions.foldLeft(str) { case (cur, (from, to)) => cur.replaceAll(from, to)}
  }

  def filtering(unifiedResult: List[String], triple:List[(String, String, String)]): List[(String, String, String)] = {
    val filteredTriple = scala.collection.mutable.ArrayBuffer.empty[List[(String, String, String)]]

    for(result <- unifiedResult){
      val us = result.split(" ")(0)
      val up = result.split(" ")(1).split(":")(1)
      val uo = result.split(" ")(2)

      var mask = false

      filteredTriple += triple.filter{case (s, p, o) =>
        if(us.contains("?")){
          mask = p.contains(up) && o.contains(uo)
        }else if(uo.contains("?")){
          mask = s.contains(us) && p.contains(up)
        }else{
          mask = s.contains(us) && p.contains(up) && o.contains(uo)
        }
        mask
      }
    }
    filteredTriple.toList.flatten
  }

  def unifyResult(conditionList:List[String], variables:List[String], resultList:List[List[String]]) = {
    val unifiedTriple = scala.collection.mutable.ArrayBuffer.empty[String]

    for(result <- resultList){
      for(condition <- conditionList){
        var temp = condition
        for(i <- 0 to variables.length - 1){
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
      try{
        val tokens = TripleParser.findAllIn(line)
        val (s, p, o) = (tokens.next(), tokens.next(), tokens.next())
        (s, p, o)

      }catch {
        case nse: NoSuchElementException => {
          ("ERROR", "ERROR", "ERROR")
        }
      }
    }

  }
}
