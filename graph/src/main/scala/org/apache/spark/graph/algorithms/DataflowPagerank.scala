package org.apache.spark.graph.algorithms

import org.apache.spark._


object DataflowPageRank extends Logging {

  def main(args: Array[String]) = {
    val host = args(0)
    val fname = args(1)
    val edgePartitions = if (args.length >= 3) args(3) else 64
    val sc = new SparkContext(host, "Dataflow PageRank(" + fname + ")")
    val edges = sc.textFile(fname, minEdgePartitions).mapPartitions( iter =>
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
    val ranks = DataflowPageRank.run(edges)
    println(ranks.count + " pages in graph")
    println("Runtime: " + (System.currentTimeMillis - start)/1000.0)
    val topRanks = ranks.top(30)(Ordering.by((entry: (Long, Double)) => entry._2))
    println(topRanks.deep.mkString("\n"))
    
  }

  // For now edge data is arbitrarily a string
  def run(edges: RDD[(Long, (Long, String))]): RDD[(Long, Double)] = {
      val numIter = 20
      val alpha: 0.15

      // val edgesWithWeights: RDD= = edges.reduceByKey { case (src: Long, (dst: Long, _: ED)) => 


      // val edges: RDD[(src, dst, data)]


      // edges (src, dst, data)
      // 
      // v: RDD[(Vid, rank)] = edges.map.(e => src, 1).union(edges.map(e => dst, 1)).distinct

      // weights = edges.map(src, dst, _ => src, 1).reduceByKey(_+_)
      // 
      // ew: RDD[(src, (dst,weight))] = weights.join(edges)

      // //update

      // newRanks = ew.join(v).map {case (src:Long, ((dst: Long, w: Double), rank: Double)) => (dst, w*r)}
      //   .reduceByKey(_+_).join(v)
      //   .map { case (id: Long, (incomingRanks: Double, myRank: Double)) =>
      //     (id, alpha*myRank + (1.0-alpha)*incomingRanks)}



    val weightedEdges: RDD[(Long, (Long, Double))] =
      edges.map { case (src, (dst, _)) => (src, 1.0)}.reduceByKey(_+_).join(edges)
      .map{ case (src, ((dst, _), weight)) => (src, (dst, weight))}

    var ranks: RDD[(Long, Double)] = edges.map{ case (src, (dst, _)) => (src, 1.0)}
      .union(edges.map{ case (src, (dst, _)) => (dst, 1.0)}).distinct()

    for (i <- 1 to numIter) {
      ranks = weightedEdges.join(ranks)
        .map {case (src, ((dst, weight), rank)) => (dst, weight*rank)}
        .reduceBykey(_+_)
        .join(ranks)
        .map { case (id, (incomingRanks, myRank)) => (id, alpha*myRank + (1.0-alpha)*incomingRanks)}
       println("Finished iteration: " + i) 
    }
    ranks
  }
}
