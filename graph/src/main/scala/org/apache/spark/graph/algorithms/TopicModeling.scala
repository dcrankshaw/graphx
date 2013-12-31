package org.apache.spark.graph.algorithms

import util.Random
import org.apache.spark.graph._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.broadcast._
import org.apache.spark.util.BoundedPriorityQueue

object LDA {
  type DocId = Vid
  type WordId = Vid
  type TopicId = Int
  type Count = Int

  class Posterior (docs: VertexRDD[Array[Count]], words: VertexRDD[Array[Count]])

  /**
   * The Factor class represents a vector of counts that is nTopics long.
   *
   * The factor class internally stores a single topic assignment (e.g., [0 0 0 1 0 0]) as
   * a scalar (i.e., topicId = 3) rather than allocating an array.  This optimization is
   * needed to efficienlty support adding token assignments to count vectors.
   *
   * The Factor class is a member of the LDA class because it relies on the nTopics member
   * variable.
   *
   * @param topic
   * @param counts
   */
  class Factor(private val nTopics: TopicId = 0, private var topic: TopicId = -1,
               private var counts: Array[Count] = null) extends Serializable {

    def this(nTopics: TopicId, topic: TopicId) = this(nTopics, topic, null)
    def this(nTopics: TopicId, counts: Array[Count]) = this(nTopics, -1, counts)

    def empty: Boolean = (topic == -1 && counts == null)

    def makeDense() {
      if (counts == null) {
        counts = new Array[Count](nTopics)
        if (topic >= 0) counts(topic) = 1
        topic = -1
      }
    }

    def +=(other: Factor) {
      makeDense()
      if(!other.empty) {
        if (other.counts == null) {
          counts(other.topic) += 1
        } else {
          var i = 0
          assert(counts ne other.counts)
          while (i < other.counts.size) {
            counts(i) += other.counts(i)
            i += 1
          }
        }
      }
    }

    def asCounts(): Array[Count] = {
      makeDense()
      counts
    }
  } // end of Factor

} // end of LDA singleton


class LDA(@transient val tokens: RDD[(LDA.WordId, LDA.DocId)],
          val nTopics: Int = 100,
          val alpha: Double = 0.1,
          val beta: Double = 0.1) extends Serializable {
  import LDA._

  @transient private val sc = tokens.sparkContext

  /**
   * The bipartite terms by document graph.
   */
  @transient var graph: Graph[Factor, TopicId] = {
    // To setup a bipartite graph it is necessary to ensure that the document and
    // word ids are in a different namespace
    val renumbered = tokens.map { case (wordId, docId) =>
      assert(wordId >= 0)
      assert(docId >= 0)
      val newDocId: DocId = -(docId + 1L)
      (wordId, newDocId)
    }
    Graph.fromEdgeTuples(renumbered, false)
      .mapVertices((id, _) => new Factor(nTopics))
      .mapEdges(e => Random.nextInt(nTopics)).cache
  }

  def verify() {
    val vfactor = graph.vertices.filter{ case (id, f) => (id == 1) }.map(p => p._2.asCounts).collect.first
    val edges = graph.edges.filter(e => e.srcId == 1).map(e => e.attr).collect
    updateCounts()
    val vfactor1 = graph.vertices.filter{ case (id, f) => (id == 1) }.map(p => p._2.asCounts).collect.first
    val edges1 = graph.edges.filter(e => e.srcId == 1).map(e => e.attr).collect
    println(edges)
    println(vfactor)

//
//    val f = graph.vertices.map { case (id, f) => f }.reduce{ (a, b) => a += b; a }
//    for (c <- f.asCounts) {
//      assert( c >= 0 )
//      assert( c % 2 == 0 )
//    }
//    assert(f.asCounts.sum % 2 == 0)
//    assert(f.asCounts.sum / 2 == graph.numEdges)

  }

  /**
   * The number of unique words in the corpus
   */
  val nwords = graph.vertices.filter{ case (vid, c) => vid >= 0 }.count()

  /**
   * The number of documents in the corpus
   */
  val ndocs = graph.vertices.filter{ case (vid, c) => vid < 0 }.count()

  /**
   * The total counts for each topic
   */
  var topicC: Broadcast[Array[Count]] = null

  // Execute update counts after initializing all the member
  updateCounts()


  /**
   * The update counts function updates the term and document counts in the
   * graph as well as the overall topic count based on the current topic
   * assignments of each token (the edge attributes).
   *
   */
  def updateCounts() {
    implicit object FactorAccumParam extends AccumulatorParam[Factor] {
      def addInPlace(a: Factor, b: Factor): Factor = { a += b; a }
      def zero(initialValue: Factor): Factor = new Factor(nTopics)
    }
    val accum = sc.accumulator(new Factor(nTopics))

    def mapFun(e: EdgeTriplet[Factor, TopicId]): Iterator[(Vid, Factor)] = {
      assert(e.attr >= 0)
      accum += new Factor(nTopics, e.attr)
      Iterator((e.srcId, new Factor(nTopics, e.attr)), (e.dstId, new Factor(nTopics, e.attr)))
    }
    val newCounts = graph.mapReduceTriplets[Factor](mapFun, (a, b) => { a += b; a } )
    graph = graph.outerJoinVertices(newCounts) { (vid, oldFactor, newFactorOpt) => newFactorOpt.get }
    // Trigger computation of the topic counts
    // TODO: We should uncache the graph at some point.
    graph.cache
    graph.vertices.foreach(x => ())
    val globalCounts: Factor = accum.value
    topicC = sc.broadcast(globalCounts.asCounts())
  } // end of update counts

  def topWords(k: Int): Array[Array[(Count, WordId)]] = {
    graph.vertices.filter {
      case (vid, c) => vid >= 0
    }.mapPartitions { items =>
      val queues = Array.fill(nTopics)(new BoundedPriorityQueue[(Count, WordId)](k))
      for ((wordId, factor) <- items) {
        var t = 0
        val counts: Array[Count] = factor.asCounts()
        while (t < nTopics) {
          val tpl: (Count, WordId) = (counts(t), wordId)
          queues(t) += tpl
          t += 1
        }
      }
      Iterator(queues)
    }.reduce { (q1, q2) =>
      q1.zip(q2).foreach { case (a,b) => a ++= b }
      q1
    }.map ( q => q.toArray )
  } // end of TopWords


  private def sampleToken(triplet: EdgeTriplet[Factor, TopicId]): TopicId = {
    val w: Array[Count] = triplet.srcAttr.asCounts()
    val d: Array[Count] = triplet.dstAttr.asCounts()
    assert(w ne d)
    assert(triplet.srcId >= 0)
    assert(triplet.dstId < 0)
    val total: Array[Count] = topicC.value
    val oldTopic = triplet.attr
    if(triplet.srcId != 1) {
      return oldTopic
    }

    // Subtract out the old assignment from the counts
    if(triplet.srcId == 1) {
      println(w.sum)
    }
    if(w(oldTopic) <= 0) {
      println("Strange error")
    }
    assert(w(oldTopic) > 0)
    assert(d(oldTopic) > 0)
    assert(total(oldTopic) > 0)
    w(oldTopic) -= 1
    d(oldTopic) -= 1
    total(oldTopic) -= 1
    // Construct the conditional
    val conditional = new Array[Double](nTopics)
    var t = 0
//    var conditionalSum = 0.0
//    while (t < conditional.size) {
//      conditional(t) = (alpha + d(t)) * (beta + w(t)) / (beta * nwords + total(t))
//      conditionalSum += conditional(t)
//      t += 1
//    }
//    assert(conditionalSum > 0.0)
//    // Generate a random number between [0, conditionalSum)
//    val u = Random.nextDouble() * conditionalSum
//    assert(u < conditionalSum)
//    // Draw the new topic from the multinomial
//    t = 0
//    var cumsum = conditional(t)
//    while(cumsum < u) {
//      t += 1
//      cumsum += conditional(t)
//    }
    val newTopic = t
    // Cheat !!!! and modify the vertex and edge attributes in place
    w(newTopic) += 1
    d(newTopic) += 1
    total(newTopic) += 1 // <-- This might be dangerous
    // Return the new topic
    newTopic
  }

  /**
   * Run the gibbs sampler
   * @param nIter
   * @return
   */
  def iterate(nIter: Int = 1) {
    // Run the sampling
    for (i <- 0 until nIter) {
      println("Starting iteration: " + i.toString)
      // Resample all the tokens
      graph = graph.mapTriplets(sampleToken _)
      updateCounts() // <-- This is an action
      println("Sampled iteration: " + i.toString)
    }
  }

  def posterior: Posterior = {
    graph.cache()
    val words = graph.vertices.filter { case (vid, _) => vid >= 0 }.mapValues(_.asCounts())
    val docs =  graph.vertices.filter { case (vid,_) => vid < 0 }.mapValues(_.asCounts())
    new LDA.Posterior(words, docs)
  }

} // end of TopicModeling



object TopicModeling {
  def main(args: Array[String]) {
    val host = "local" // args(0)
    val options =  args.drop(3).map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    var tokensFile = "/Users/jegonzal/Data/counts.tsv"
    var dictionaryFile = "/Users/jegonzal/Data/dictionary.txt"
    var numVPart = 4
    var numEPart = 4
    var partitionStrategy: Option[PartitionStrategy] = None
    var nIter = 5
    var nTopics = 10
    var alpha = 0.1
    var beta  = 0.1

    def pickPartitioner(v: String): PartitionStrategy = v match {
      case "RandomVertexCut" => RandomVertexCut
      case "EdgePartition1D" => EdgePartition1D
      case "EdgePartition2D" => EdgePartition2D
      case "CanonicalRandomVertexCut" => CanonicalRandomVertexCut
      case _ => throw new IllegalArgumentException("Invalid Partition Strategy: " + v)
    }

    options.foreach{
      case ("tokens", v) => tokensFile = v
      case ("dictionary", v) => dictionaryFile = v
      case ("numVPart", v) => numVPart = v.toInt
      case ("numEPart", v) => numEPart = v.toInt
      case ("partStrategy", v) => partitionStrategy = Some(pickPartitioner(v))
      case ("niter", v) => nIter = v.toInt
      case ("ntopics", v) => nTopics = v.toInt
      case ("alpha", v) => alpha = v.toDouble
      case ("beta", v) => beta = v.toDouble
      case (opt, _) => throw new IllegalArgumentException("Invalid option: " + opt)
    }


    // def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    //   loggers.map{
    //     loggerName =>
    //       val logger = org.apache.log4j.Logger.getLogger(loggerName)
    //     val prevLevel = logger.getLevel()
    //     logger.setLevel(level)
    //     loggerName -> prevLevel
    //   }.toMap
    // }
    // setLogLevels(org.apache.log4j.Level.DEBUG, Seq("org.apache.spark"))


    val serializer = "org.apache.spark.serializer.KryoSerializer"
    System.setProperty("spark.serializer", serializer)
    //System.setProperty("spark.shuffle.compress", "false")
    System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")
    val sc = new SparkContext(host, "LDA(" + tokensFile + ")")

    val rawTokens: RDD[(LDA.WordId, LDA.DocId)] =
      sc.textFile(tokensFile, numEPart).flatMap { line =>
      val lineArray = line.split("\\s+")
      if(lineArray.length != 3) {
        println("Invalid line: " + line)
        assert(false)
      }
      val termId = lineArray(0).trim.toLong
      val docId = lineArray(1).trim.toLong
      assert(termId >= 0)
      assert(docId >= 0)
      val count = lineArray(2).trim.toInt
      assert(count > 0)
      //Iterator((termId, docId))
      Iterator.fill(count)((termId, docId))
    }

//    val rawTokens: RDD[(LDA.WordId, LDA.DocId)] =
//      sc.parallelize(Array((0L,10L, 10), (0L,11L, 10), (1L, 10L, 1), (1L, 12L, 1)), 4).flatMap {
//        case (termId, docId, count) =>
//        assert(termId >= 0)
//        assert(docId >= 0)
//        assert(count > 0)
//        // Iterator((termId, docId))
//        Iterator.fill(count)((termId, docId))
//      }

    val model = new LDA(rawTokens, nTopics, alpha, beta)
    model.verify()
    model.iterate(nIter)

    sc.stop()

  }
} // end of TopicModeling object

