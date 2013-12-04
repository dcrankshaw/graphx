package org.apache.spark.graph.examples

import org.apache.spark._
import org.apache.spark.graph._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.mahout.text.wikipedia._
import org.apache.spark.rdd.RDD
import java.util.Calendar


object AnalyzeWikipedia extends Logging {

  def main(args: Array[String]) = {




    val host = args(0)
    val fname = args(1)
    val numparts = {
      if (args.length >= 3) {
        args(2).toInt
      } else {
        64
      }
    }

   val serializer = "org.apache.spark.serializer.KryoSerializer"
   System.setProperty("spark.serializer", serializer)
   System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

    val sc = new SparkContext(host, "AnalyzeWikipedia")


    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ");
    conf.set("xmlinput.start", "<page>");
    conf.set("xmlinput.end", "</page>");

    val xmlRDD = sc.newAPIHadoopFile(fname, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(stringify)

    println("XML pages: " + xmlRDD.count)
      // .repartition(numparts)

    val wikiRDD = xmlRDD.map { raw => new WikiArticle(raw) }
      .filter { art => art.relevant }

    println("Relevant pages: " + wikiRDD.count)

    val vertices = wikiRDD.map { art => (art.vertexID, art.title) }

    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    println("Edges: " + edges.count)
    println("Creating graph: " + Calendar.getInstance().getTime())

    val g = Graph(vertices, edges)
    g.triplets.count
    try {
      println("starting pagerank" + Calendar.getInstance().getTime())
      val startTime = System.currentTimeMillis
      val pr = Analytics.pagerank(g, 10)
      println("Pagerank runtime:    " + ((System.currentTimeMillis - startTime)/1000.0) + " seconds")
    // val ct = g.triplets.count
    // println(ct)
    } catch {
      case ex => {
        ex.printStackTrace()
      }
      
    } finally {
      sc.stop()
    }
  }


  def stringify(tup: (org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text)): String = {
    tup._2.toString
  }



}
