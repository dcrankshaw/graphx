package org.apache.spark.graphx

import org.apache.spark._
// import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.hadoop.io.{LongWritable, Text}
// import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.mahout.text.wikipedia._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import java.util.{HashSet => JHashSet, TreeSet => JTreeSet}

object WikiPipelineBenchmarkSimplified extends Logging {

  def main(args: Array[String]) = {

    val host = args(0)

    val sparkconf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator")

    val sc = new SparkContext(host, "WikiPipeline", sparkconf)

    val start = System.currentTimeMillis
    val rawData = args(2)
    val numIters = args(3).toInt
    val numPRIters = args(4).toInt
    benchmarkGraphx(sc, rawData, numIters, numPRIters)
    logWarning("TIMEX: " + (System.currentTimeMillis - start)/1000.0)
    sc.stop()
    System.exit(0)
  }

  def benchmarkGraphx(sc: SparkContext, rawData: String, numIters: Int, numPRIters: Int) {
    val (vertices, edges) = extractLinkGraph(sc, rawData)
    logWarning("creating graph")
    val g = Graph(vertices, edges)
    val cleanG = g.subgraph(x => true, (vid, vd) => vd != null).cache
    logWarning(s"DIRTY graph has ${g.triplets.count()} EDGES, ${g.vertices.count()} VERTICES")
    logWarning(s"CLEAN graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
    val resultG = pagerankConnComponentsAlt(numIters, cleanG, numPRIters).cache()
    logWarning(s"ORIGINAL graph has ${cleanG.triplets.count()} EDGES, ${cleanG.vertices.count()} VERTICES")
    logWarning(s"FINAL graph has ${resultG.triplets.count()} EDGES, ${resultG.vertices.count()} VERTICES")
  }

  def pagerankConnComponentsAlt(numRepetitions: Int, g: Graph[String, Double], numPRIters: Int): Graph[String, Double] = {
    var currentGraph = g
    logWarning("starting iterations")
    for (i <- 0 to numRepetitions) {
      currentGraph.cache
      val startTime = System.currentTimeMillis
      // GRAPH VIEW
      val workGraph = currentGraph.cloneVertices().cache()
      workGraph.vertices.count()
      val ccStartTime = System.currentTimeMillis
      val ccGraph = ConnectedComponents.run(workGraph).cache
      val numCCs = ccGraph.vertices.map{ case (id, cc) => cc }.distinct(1).count
      val ccEndTime = System.currentTimeMillis
      logWarning(s"Connected Components TIMEX: ${(ccEndTime - ccStartTime)/1000.0}")
      logWarning(s"Number of connected components for iteration $i: $numCCs")

      val prWorkGraph = currentGraph.cloneVertices().cache()
      prWorkGraph.vertices.count()
      val prStartTime = System.currentTimeMillis
      val pr = PageRank.run(prWorkGraph, numPRIters).cache
      pr.vertices.count
      val prEndTime = System.currentTimeMillis
      logWarning(s"Pagerank TIMEX: ${(prEndTime - prStartTime)/1000.0}")
      logWarning("Pagerank completed")

      // TABLE VIEW

      val prAndTitle = currentGraph.outerJoinVertices(pr.vertices)({(id: VertexId, title: String, rank: Option[Double]) => (title, rank.getOrElse(0.0))}).cache
      prAndTitle.vertices.count
      // logWarning("join completed.")
      val top20 = prAndTitle.vertices.top(20)(Ordering.by((entry: (VertexId, (String, Double))) => entry._2._2))
      logWarning(s"Top20 for iteration $i:\n${top20.mkString("\n")}")
      // val top20verts = top20.map(_._1).toSet
      // // filter out top 20 vertices
      // val filterTop20 = {(v: VertexId, d: String) =>
      //   !top20verts.contains(v)
      // }
      // val newGraph = currentGraph.subgraph(x => true, filterTop20).cache
      // newGraph.vertices.count

      logWarning(s"TOTAL_TIMEX iter $i ${(System.currentTimeMillis - startTime)/1000.0}")
      // currentGraph = newGraph

    }
    currentGraph
  }

  def extractLinkGraph(sc: SparkContext, rawData: String): (RDD[(VertexId, String)], RDD[Edge[Double]]) = {
    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ")
    conf.set("xmlinput.start", "<page>")
    conf.set("xmlinput.end", "</page>")

    logWarning("about to load xml rdd")
    val xmlRDD = sc.newAPIHadoopFile(rawData, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(t => t._2.toString)
    // xmlRDD.count
    logWarning(s"XML RDD counted. Found ${xmlRDD.count} raw articles.")
    val repartXMLRDD = xmlRDD.repartition(128)
    logWarning(s"XML RDD repartitioned. Found ${repartXMLRDD.count} raw articles.")

    val allArtsRDD = repartXMLRDD.map { raw => new WikiArticle(raw) }.cache
    logWarning(s"Total articles: Found ${allArtsRDD.count} UNPARTITIONED articles.")

    val wikiRDD = allArtsRDD.filter { art => art.relevant }.cache //.repartition(128)
    logWarning(s"wikiRDD counted. Found ${wikiRDD.count} relevant articles in ${wikiRDD.partitions.size} partitions")
    val vertices: RDD[(VertexId, String)] = wikiRDD.map { art => (art.vertexID, art.title) }
    val edges: RDD[Edge[Double]] = wikiRDD.flatMap { art => art.edges }
    (vertices, edges)

  }
}
