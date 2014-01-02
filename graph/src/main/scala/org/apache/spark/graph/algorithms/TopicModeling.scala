package org.apache.spark.graph.algorithms

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


  type Factor = Array[Count]

  class Posterior (docs: VertexRDD[Factor], words: VertexRDD[Factor])

  def addEq(a: Factor, b: Factor): Factor = {
    assert(a.size == b.size)
    var i = 0
    while (i < a.size) {
      a(i) += b(i)
      i += 1
    }
    a
  }

  def addEq(a: Factor, t: TopicId): Factor = { a(t) += 1; a }

  def makeFactor(nTopics: Int, topic: TopicId): Factor = {
    val f = new Factor(nTopics)
    f(topic) += 1
    f
  }

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
//  class Factor(private val nTopics: TopicId = 0, private var topic: TopicId = -1,
//               private var counts: Array[Count] = null) extends Serializable {
//
//    def this(nTopics: TopicId, topic: TopicId) = this(nTopics, topic, null)
//    def this(nTopics: TopicId, counts: Array[Count]) = this(nTopics, -1, counts)
//
//    def empty: Boolean = (topic == -1 && counts == null)
//
//    def makeDense() {
//      if (counts == null) {
//        counts = new Array[Count](nTopics)
//        if (topic >= 0) counts(topic) = 1
//        topic = -1
//      }
//    }
//
//    def +=(other: Factor) {
//      makeDense()
//      if(!other.empty) {
//        if (other.counts == null) {
//          counts(other.topic) += 1
//        } else {
//          var i = 0
//          assert(counts ne other.counts)
//          while (i < other.counts.size) {
//            counts(i) += other.counts(i)
//            i += 1
//          }
//        }
//      }
//    }
//
//    def asCounts(): Array[Count] = {
//      makeDense()
//      counts
//    }
//  } // end of Factor

} // end of LDA singleton





class LDA(@transient val tokens: RDD[(LDA.WordId, LDA.DocId)],
          val nTopics: Int = 100,
          val alpha: Double = 0.1,
          val beta: Double = 0.1) {
  import LDA._

  private val sc = tokens.sparkContext

  /**
   * The bipartite terms by document graph.
   */
  var graph: Graph[Factor, TopicId] = {
    // To setup a bipartite graph it is necessary to ensure that the document and
    // word ids are in a different namespace
    val renumbered = tokens.map { case (wordId, docId) =>
      assert(wordId >= 0)
      assert(docId >= 0)
      val newDocId: DocId = -(docId + 1L)
      (wordId, newDocId)
    }
    val nT = nTopics
    // Sample the tokens
    val gTmp = Graph.fromEdgeTuples(renumbered, false).mapEdges { (pid, iter) =>
        val gen = new java.util.Random(pid)
        iter.map(e => gen.nextInt(nT))
      }
    // Compute the topic histograms (factors) for each word and document
    val newCounts = gTmp.mapReduceTriplets[Factor](
      e => Iterator((e.srcId, makeFactor(nT, e.attr)), (e.dstId, makeFactor(nT, e.attr))),
      (a, b) => addEq(a,b) )
    // Update the graph with the factors
    gTmp.outerJoinVertices(newCounts) { (_, _, newFactorOpt) => newFactorOpt.get }
    // Trigger computation of the topic counts
  }

  /**
   * The number of unique words in the corpus
   */
  val nWords = graph.vertices.filter{ case (vid, c) => vid >= 0 }.count()

  /**
   * The number of documents in the corpus
   */
  val nDocs = graph.vertices.filter{ case (vid, c) => vid < 0 }.count()

  /**
   * The number of tokens
   */
  val nTokens = graph.edges.count()

  /**
   * The total counts for each topic
   */
  var totalHist = graph.edges.map(e => e.attr)
    .aggregate(new Factor(nTopics))(LDA.addEq(_, _), LDA.addEq(_, _))
  assert(totalHist.sum == nTokens)

  /**
   * The internal iteration tracks the number of times the random number
   * generator was created.  In constructing the graph the generated is created
   * once and then once for each iteration
   */
  private var internalIteration = 1


  /**
   * Run the gibbs sampler
   * @param nIter
   * @return
   */
  def iterate(nIter: Int = 1) {
    // Run the sampling
    for (i <- 0 until nIter) {
      println("Starting iteration: " + i.toString)
      if(i == 2) {
        println("here comes the error")
      }
      // Broadcast the topic histogram
      val totalHistbcast = sc.broadcast(totalHist)
      // Shadowing because scala's closure capture is an abomination
      val a = alpha
      val b = beta
      val nt = nTopics
      val nw = nWords
      // Define the function to sample a single token
      // I had originally used def sampleToken but this leads to closure capture of the LDA class (an abomination).
      val sampleToken = (gen: java.util.Random, triplet: EdgeTriplet[Factor, TopicId]) => {
        val wHist: Array[Count] = triplet.srcAttr
        val dHist: Array[Count] = triplet.dstAttr
        val totalHist: Array[Count] = totalHistbcast.value
        val oldTopic = triplet.attr
        assert(wHist(oldTopic) > 0)
        assert(dHist(oldTopic) > 0)
        assert(totalHist(oldTopic) > 0)
        // Construct the conditional
        val conditional = new Array[Double](nt)
        var t = 0
        var conditionalSum = 0.0
        while (t < conditional.size) {
          val cavityOffset = if (t == oldTopic) 1 else 0
          val w = wHist(t) - cavityOffset
          val d = dHist(t) - cavityOffset
          val total = totalHist(t) - cavityOffset
          conditional(t) = (a + d) * (b + w) / (b * nw + total)
          conditionalSum += conditional(t)
          t += 1
        }
        assert(conditionalSum > 0.0)
        // Generate a random number between [0, conditionalSum)
        val u = gen.nextDouble() * conditionalSum
        assert(u < conditionalSum)
        // Draw the new topic from the multinomial
        t = 0
        var cumsum = conditional(t)
        while(cumsum < u) {
          t += 1
          cumsum += conditional(t)
        }
        val newTopic = t
        // Return the new topic
        newTopic
      }

      // Resample all the tokens
      val parts = graph.edges.partitions.size
      val interIter = internalIteration
      graph = graph.mapTriplets { (pid, iter) =>
        val gen = new java.util.Random(parts * interIter + pid)
        iter.map(token => sampleToken(gen, token))
      }

      // Update the counts
      val newCounts = graph.mapReduceTriplets[Factor](
        e => Iterator((e.srcId, makeFactor(nt, e.attr)), (e.dstId, makeFactor(nt, e.attr))),
        (a, b) => { addEq(a,b); a } )
      graph = graph.outerJoinVertices(newCounts) { (_, _, newFactorOpt) => newFactorOpt.get }

      // Recompute the global counts (the actual action)
      totalHist = graph.edges.map(e => e.attr)
        .aggregate(new Factor(nt))(LDA.addEq(_, _), LDA.addEq(_, _))
      assert(totalHist.sum == nTokens)

      internalIteration += 1
      println("Sampled iteration: " + i.toString)
    }
  } // end of iterate



//
//  /**
//   * The update counts function updates the term and document counts in the
//   * graph as well as the overall topic count based on the current topic
//   * assignments of each token (the edge attributes).
//   *
//   */
//  def updateCounts() {
//    implicit object FactorAccumParam extends AccumulatorParam[Factor] {
//      def addInPlace(a: Factor, b: Factor): Factor = { a += b; a }
//      def zero(initialValue: Factor): Factor = new Factor(nTopics)
//    }
//    val accum = sc.accumulator(new Factor(nTopics))
//
//    def mapFun(e: EdgeTriplet[Factor, TopicId]): Iterator[(Vid, Factor)] = {
//      assert(e.attr >= 0)
//      accum += new Factor(nTopics, e.attr)
//      Iterator((e.srcId, new Factor(nTopics, e.attr)), (e.dstId, new Factor(nTopics, e.attr)))
//    }
//    val newCounts = graph.mapReduceTriplets[Factor](mapFun, (a, b) => { a += b; a } )
//    graph = graph.outerJoinVertices(newCounts) { (vid, oldFactor, newFactorOpt) => newFactorOpt.get }.cache
//    // Trigger computation of the topic counts
//    // TODO: We should uncache the graph at some point.
//    //graph.cache
//    graph.vertices.foreach(x => ())
//    val globalCounts: Factor = accum.value
//    assert(globalCounts.asCounts().sum == ntokens)
//    topicC = sc.broadcast(globalCounts.asCounts())
//  } // end of update counts

//  def topWords(k: Int): Array[Array[(Count, WordId)]] = {
//    graph.vertices.filter {
//      case (vid, c) => vid >= 0
//    }.mapPartitions { items =>
//      val queues = Array.fill(nTopics)(new BoundedPriorityQueue[(Count, WordId)](k))
//      for ((wordId, factor) <- items) {
//        var t = 0
//        val counts: Array[Count] = factor.asCounts()
//        while (t < nTopics) {
//          val tpl: (Count, WordId) = (counts(t), wordId)
//          queues(t) += tpl
//          t += 1
//        }
//      }
//      Iterator(queues)
//    }.reduce { (q1, q2) =>
//      q1.zip(q2).foreach { case (a,b) => a ++= b }
//      q1
//    }.map ( q => q.toArray )
//  } // end of TopWords





  def posterior: Posterior = {
    graph.cache()
    val words = graph.vertices.filter { case (vid, _) => vid >= 0 }
    val docs =  graph.vertices.filter { case (vid,_) => vid < 0 }
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
    var nIter = 3
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
  //  model.verify()
    model.iterate(nIter)

    sc.stop()

  }
} // end of TopicModeling object

