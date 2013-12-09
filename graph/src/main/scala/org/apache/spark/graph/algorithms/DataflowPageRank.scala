package org.apache.spark.graph.algorithms

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._


object DataflowPageRank extends Logging {

  def main(args: Array[String]) = {
    println("Starting pagerank.")
    val host = args(0)
    val fname = args(1)
    // val edgePartitions = if (args.length >= 3) args(2).trim.toInt else 128
    val edgePartitions = 128
    val sc = new SparkContext(host, "Dataflow PageRank(" + fname + ")")
    val edges = sc.textFile(fname, edgePartitions).mapPartitions( iter =>
      iter.filter(line => !line.isEmpty && line(0) != '#').map { line =>
        val lineArray = line.split("\\s+")
        if(lineArray.length < 2) {
          println("Invalid line: " + line)
          assert(false)
        }
        val source = lineArray(0).trim.toLong
        val target = lineArray(1).trim.toLong
        (source, (target, "edge"))
      })
    val start = System.currentTimeMillis
    logWarning("Starting pagerank")
    val ranks = DataflowPageRank.run(edges)
    println(ranks.count + " pages in graph")
    println("TIMEX: " + (System.currentTimeMillis - start)/1000.0)
    val topRanks = ranks.top(30)(Ordering.by((entry: (Long, Double)) => entry._2))
    println(topRanks.deep.mkString("\n"))
    
  }

  // For now edge data is arbitrarily a string
  def run(edges: RDD[(Long, (Long, String))]): RDD[(Long, Double)] = {
      val numIter = 20
      val alpha = 0.15
      val initialRank = 1.0


    // get outdegree of each each and make weight 1/outdegree
    val weightedEdges: RDD[(Long, (Long, Double))] =
      edges
      .map { case (src: Long, (dst: Long, _: String)) => (src, 1.0)}
      // .reduceByKey((v1, v2) => v1 + v2)
      .reduceByKey(_ + _)
      .join(edges)
      .map{ case (src: Long, (outDegree: Double, (dst: Long, _: String))) => (src, (dst, 1.0/outDegree))}

    // initialize ranks
    var ranks: RDD[(Long, Double)] =
      edges.map{ case (src: Long, (dst: Long, _: String)) => (src, initialRank)}
      .union(edges.map{ case (src: Long, (dst: Long, _: String)) => (dst, initialRank)})
      .distinct()

    for (i <- 1 to numIter) {
      ranks = weightedEdges.join(ranks)
        .map {case (src: Long, ((dst: Long, weight: Double), rank: Double)) => (dst, weight*rank)}
        .reduceByKey(_ + _)
        .join(ranks)
        .map { case (id: Long, (incomingRanks: Double, myRank: Double)) => (id, alpha*myRank + (1.0-alpha)*incomingRanks)}
      ranks.count
      logWarning("Finished iteration: " + i) 
    }
    ranks
  }
}
