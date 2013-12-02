import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.mahout.text.wikipedia._

val conf = new Configuration
conf.set("key.value.separator.in.input.line", " ");
conf.set("xmlinput.start", "<page>");
conf.set("xmlinput.end", "</page>");
// val path = args(1)
val path = "hdfs://ec2-54-204-189-44.compute-1.amazonaws.com:9000/enwiki-latest-pages-articles.xml"

val wikiRDD = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)

val ct = wikiRDD.count()

val first = wikiRDD.first
val anarch = wikiRDD.take(2)(1)

def stringify(tup: (org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text)): String = {
  tup._2.toString
}

    // println(ct)
// 



////////////////////////////////////////////////////////////

import java.util.regex.Pattern
import java.util.regex.Matcher
import scala.util.matching.Regex
import scala.collection.mutable
import scala.xml._

case class WikiArticle(wtext: String, links: Array[String], redirect: Boolean, stub: Boolean, disambig: Boolean, relevant: Boolean, title: String) {



}

object WikiArticle {
  val titlePattern = "<title>(.*)<\\/title>".r
  val redirectPattern = "#REDIRECT\\s+\\[\\[(.*?)\\]\\]".r
  val disambigPattern = "\\{\\{disambig\\}\\}".r
  val stubPattern = "\\-stub\\}\\}".r
  val linkPattern = Pattern.compile("\\[\\[(.*?)\\]\\]", Pattern.MULTILINE) 

  def apply(wt: String): WikiArticle = {
    var red = false
    var st = false
    var dis = false
    var rel = false
    var li = null.asInstanceOf[Array[String]]
    if (!redirectPattern.findFirstIn(wt).isEmpty) { red = true }
    if (!stubPattern.findFirstIn(wt).isEmpty) { st = true }
    if (!disambigPattern.findFirstIn(wt).isEmpty) { dis = true }
    // only relevant if article is not a stub, disambiguation, or redirect
    rel = !(st || dis || red)
    if (rel) {
      li = parseLinks(wt)
    }
    // from https://github.com/apache/mahout/blob/mahout-0.8/integration/src/main/java/org/apache/mahout/text/wikipedia/WikipediaMapper.java
    val tiXML = titlePattern.findFirstIn(wt).getOrElse("")
    val ti = XML.loadString(tiXML).text
    new WikiArticle(wt, li, red, st, dis, rel, ti)
  }


  private def parseLinks(wt: String): Array[String] = {
    val linkBuilder = new mutable.ArrayBuffer[String]()
    val matcher: Matcher = linkPattern.matcher(wt)
    while (matcher.find()) {
      val temp: Array[String] = matcher.group(1).split("\\|")
      println(temp.deep.mkString("\n"))
      if (temp != null && temp.length == 0) {
        val link: String = temp(0)
        if (link.contains(":") == false) {
          linkBuilder += link
        }
      }
    }
    return linkBuilder.toArray
  }

}
