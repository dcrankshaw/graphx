package org.apache.spark.graph.examples

import java.util.regex.Pattern
import org.apache.spark.graph._
import java.util.regex.Matcher
import scala.util.matching.Regex
import scala.collection.mutable
import scala.xml._
import org.apache.spark.serializer.KryoRegistrator
// import org.apache.spark.util.collection.OpenHashSet
import scala.collection.immutable.HashSet



class WikiArticle(wtext: String) extends Serializable {
  @transient val links: Array[String] = WikiArticle.parseLinks(wtext)
  @transient val neighbors = links.map(WikiArticle.titleHash).distinct
  @transient lazy val redirect: Boolean = !WikiArticle.redirectPattern.findFirstIn(wtext).isEmpty
  @transient lazy val stub: Boolean = !WikiArticle.stubPattern.findFirstIn(wtext).isEmpty
  @transient lazy val disambig: Boolean = !WikiArticle.disambigPattern.findFirstIn(wtext).isEmpty
  @transient lazy val tiXML = WikiArticle.titlePattern.findFirstIn(wtext).getOrElse("")
  val title: String = {
    try {
      XML.loadString(tiXML).text
    } catch {
      case e => "" // don't use null because we get null pointer exceptions
    }
  }
  val relevant: Boolean = !(redirect || stub || disambig || title == null)
  val vertexID: Vid = WikiArticle.titleHash(title)
  val edges: HashSet[Edge[Double]] = {
    val temp = neighbors.map { n => Edge(vertexID, n, 1.0) }
    val set = new HashSet[Edge[Double]]() ++ temp
    set
  }
}

object WikiArticle {
  @transient val titlePattern = "<title>(.*)<\\/title>".r
  @transient val redirectPattern = "#REDIRECT\\s+\\[\\[(.*?)\\]\\]".r
  @transient val disambigPattern = "\\{\\{disambig\\}\\}".r
  @transient val stubPattern = "\\-stub\\}\\}".r
  @transient val linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE) 

  private def parseLinks(wt: String): Array[String] = {
    val linkBuilder = new mutable.ArrayBuffer[String]()
    val matcher: Matcher = linkPattern.matcher(wt)
    while (matcher.find()) {
      val temp: Array[String] = matcher.group(1).split("\\|")
      // println(temp.deep.mkString("\n"))
      if (temp != null && temp.length > 0) {
        val link: String = temp(0)
        if (link.contains(":") == false) {
          linkBuilder += link
        }
      }
    }
    return linkBuilder.toArray
  }

  // substitute underscores for spaces and make lowercase
  private def canonicalize(title: String): String = {
    title.trim.toLowerCase.replace(" ", "_")
  }

  // Hash of the canonical article name. Used for vertex ID.
  private def titleHash(title: String): Vid = { math.abs(canonicalize(title).hashCode) }

}


