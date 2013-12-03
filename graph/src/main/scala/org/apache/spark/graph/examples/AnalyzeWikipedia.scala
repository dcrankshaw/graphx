package org.apache.spark.graph.examples

import org.apache.spark._
import org.apache.spark.graph._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.mahout.text.wikipedia._


object AnalyzeWikipedia extends Logging {

  def main(args: Array[String]) = {




    val host = args(0)
    val fname = args(1)
    val numparts = {
      if (args.length >= 3) {
        args(2)
      } else {
        64
      }
    }


    val sc = new SparkContext(host, "AnalyzeWikipedia")


    val conf = new Configuration conf.set("key.value.separator.in.input.line", " ");
    conf.set("xmlinput.start", "<page>");
    conf.set("xmlinput.end", "</page>");

    val xmlRDD = sc.newAPIHadoopFile(fname, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(stringify)
      .repartition(numparts)

    val wikiRDD = xmlRDD.map { raw => new WikiArticle(raw) }

    println(wikiRDD.count)

    val vertices = wikiRDD.map { art => (art.vertexID, art.title) }

    val edges = wikiRDD.flatMap { art => art.edges }

    val g = Graph(vertices, edges)
  }


  def stringify(tup: (org.apache.hadoop.io.LongWritable, org.apache.hadoop.io.Text)): String = {
    tup._2.toString
  }



}
