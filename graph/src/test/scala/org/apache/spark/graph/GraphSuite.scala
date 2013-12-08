package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph.LocalSparkContext._
import org.apache.spark.rdd._

class GraphSuite extends FunSuite with LocalSparkContext {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

  test("Graph Creation") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = (0L to 100L).zip((1L to 99L) :+ 0L)
      val edges = sc.parallelize(rawEdges)
      val graph = Graph.fromEdgeTuples(edges, 1.0F)
      assert(graph.edges.count() === rawEdges.size)
    }
  }

  test("Graph Creation with invalid vertices") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val rawEdges = (0L to 98L).zip((1L to 99L) :+ 0L)
      val edges: RDD[Edge[Int]] = sc.parallelize(rawEdges).map { case (s, t) => Edge(s, t, 1) }
      val vertices: RDD[(Vid, Boolean)] = sc.parallelize((0L until 10L).map(id => (id, true)))
      val graph = Graph(vertices, edges, false)
      assert( graph.edges.count() === rawEdges.size )
      assert( graph.vertices.count() === 100)
      graph.triplets.map { et =>
        assert((et.srcId < 10 && et.srcAttr) || (et.srcId >= 10 && !et.srcAttr))
        assert((et.dstId < 10 && et.dstAttr) || (et.dstId >= 10 && !et.dstAttr))
      }
    }
  }

  test("mapEdges") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 3
      val star = Graph.fromEdgeTuples(
        sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))),
        "defaultValue")
      val starWithEdgeAttrs = star.mapEdges(e => e.dstId)

      // map(_.copy()) is a workaround for https://github.com/amplab/graphx/issues/25
      val edges = starWithEdgeAttrs.edges.map(_.copy()).collect()
      assert(edges.size === n)
      assert(edges.toSet === (1 to n).map(x => Edge(0, x, x)).toSet)
    }
  }

  test("mapReduceTriplets") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 3
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), 0)
      val starDeg = star.joinVertices(star.degrees){ (vid, oldV, deg) => deg }
      val neighborDegreeSums = starDeg.mapReduceTriplets(
        edge => Iterator((edge.srcId, edge.dstAttr), (edge.dstId, edge.srcAttr)),
        (a: Int, b: Int) => a + b)
      assert(neighborDegreeSums.collect().toSet === (0 to n).map(x => (x, n)).toSet)
    }
  }

  test("aggregateNeighbors") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val n = 3
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), 1)

      val indegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.In)
      assert(indegrees.collect().toSet === (1 to n).map(x => (x, 1)).toSet)

      val outdegrees = star.aggregateNeighbors(
        (vid, edge) => Some(1),
        (a: Int, b: Int) => a + b,
        EdgeDirection.Out)
      assert(outdegrees.collect().toSet === Set((0, n)))

      val noVertexValues = star.aggregateNeighbors[Int](
        (vid: Vid, edge: EdgeTriplet[Int, Int]) => None,
        (a: Int, b: Int) => throw new Exception("reduceFunc called unexpectedly"),
        EdgeDirection.In)
      assert(noVertexValues.collect().toSet === Set.empty[(Vid, Int)])
    }
  }

  test("joinVertices") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val vertices = sc.parallelize(Seq[(Vid, String)]((1, "one"), (2, "two"), (3, "three")), 2)
      val edges = sc.parallelize((Seq(Edge(1, 2, "onetwo"))))
      val g: Graph[String, String] = Graph(vertices, edges)

      val tbl = sc.parallelize(Seq[(Vid, Int)]((1, 10), (2, 20)))
      val g1 = g.joinVertices(tbl) { (vid: Vid, attr: String, u: Int) => attr + u }

      val v = g1.vertices.collect().toSet
      assert(v === Set((1, "one10"), (2, "two20"), (3, "three")))
    }
  }

  test("collectNeighborIds") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val chain = (0 until 100).map(x => (x, (x+1)%100) )
      val rawEdges = sc.parallelize(chain, 3).map { case (s,d) => (s.toLong, d.toLong) }
      val graph = Graph.fromEdgeTuples(rawEdges, 1.0)
      val nbrs = graph.collectNeighborIds(EdgeDirection.Both)
      assert(nbrs.count === chain.size)
      assert(graph.numVertices === nbrs.count)
      nbrs.collect.foreach { case (vid, nbrs) => assert(nbrs.size === 2) }
      nbrs.collect.foreach { case (vid, nbrs) =>
        val s = nbrs.toSet
        assert(s.contains((vid + 1) % 100))
        assert(s.contains(if (vid > 0) vid - 1 else 99 ))
      }
    }
  }

  test("VertexSetRDD") {
    withSpark(new SparkContext("local", "test")) { sc =>
      val a = sc.parallelize((0 to 100).map(x => (x.toLong, x.toLong)), 5)
      val b = VertexRDD(a).mapValues(x => -x).cache() // Allow joining b with a derived RDD of b
      assert(b.count === 101)
      assert(b.leftJoin(a){ (id, a, bOpt) => a + bOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      val c = b.aggregateUsingIndex[Long](a, (x, y) => x)
      assert(b.leftJoin(c){ (id, b, cOpt) => b + cOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      val d = c.filter(q => ((q._2 % 2) == 0))
      val e = a.filter(q => ((q._2 % 2) == 0))
      assert(d.count === e.count)
      assert(b.zipJoin(c)((id, b, c) => b + c).map(x => x._2).reduce(_+_) === 0)

    }
  }

  test("subgraph") {
    withSpark(new SparkContext("local", "test")) { sc =>
      // Create a star graph of 10 veritces.
      val n = 10
      val star = Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid))), "v")
      // Take only vertices whose vids are even
      val subgraph = star.subgraph(vpred = (vid, attr) => vid % 2 == 0)

      // We should have 5 vertices.
      assert(subgraph.vertices.collect().toSet === (0 to n / 2).map(x => (x * 2, "v")).toSet)

      // And 4 edges.
      assert(subgraph.edges.map(_.copy()).collect().toSet === (1 to n / 2).map(x => Edge(0, x * 2, 1)).toSet)
    }
  }


  test("triplets") {
    withSpark(new SparkContext("local", "test")) { sc =>

      val vertices = sc.parallelize((1 to 10).map(x => (x.toLong, 1)))
      val rawEdges = Seq(Edge(1,2,1),Edge(2,3,1),Edge(3,1,1),Edge(2,4,0),Edge(4,3,0),
                                    Edge(5,4,12345),Edge(4,6,0),Edge(6,7,7858),Edge(8,3,0),Edge(8,7,0),
                                    Edge(10,1,23554),Edge(8,9,1),Edge(9,10,1),Edge(10,8,1))
      val baselineTriples = Set(rawEdges.map { e =>
        val et= new EdgeTriplet[Int, Int] 
        et.srcId = e.srcId
        et.srcAttr = 1
        et.dstId = e.dstId
        et.dstAttr = 1
        et.attr = e.attr
        et
      })

      val edges = sc.parallelize(rawEdges)
      val g: Graph[Int,Int] = Graph(vertices, edges)
      
      val triples = g.triplets.collect.toSet
      assert(triples.size == baselineTriples.size)
      assert(triples === baselineTriples)
      
    }
  }


  test("contractEdges") {
    withSpark(new SparkContext("local", "test")) { sc =>


      val vertices = sc.parallelize((1 to 10).map(x => (x.toLong, 1)))
      val rawEdges = Seq(Edge(1,2,1),Edge(2,3,1),Edge(3,1,1),Edge(2,4,0),Edge(4,3,0),
                                    Edge(5,4,12345),Edge(4,6,0),Edge(6,7,7858),Edge(8,3,0),Edge(8,7,0),
                                    Edge(10,1,23554),Edge(8,9,1),Edge(9,10,1),Edge(10,8,1))
      val edges = sc.parallelize(rawEdges)
      val g: Graph[Int,Int] = Graph(vertices, edges)
      println("\nTriplets")
      g.triplets.collect.map( a => println(a))
      val g1 = g.contractEdges({ (et: EdgeTriplet[Int,Int]) => (et.attr == 1) },
      { (et: EdgeTriplet[Int,Int]) => et.attr }, { (u: Int, v: Int) => u + v })
      // g1.edges.collect.map(e => println(e))
      // g1.edges.count
      // println(g1.flatMap { case (ccID, triples) => triples.toList }.count)
      // println(g1.edges.count)
      println("Final graph edges")
      g1.edges.collect.map( t => println(t))
      // println("\nVertices")
      // g1.vertices.collect.map( t => println(t))
      println("\nFinal Triplets")
      g1.triplets.collect.map( a => println(a))



      //val a = sc.parallelize((0 to 100).map(x => (x.toLong, x.toLong)), 5)
      //val b = VertexSetRDD(a).mapValues(x => -x)
      //assert(b.count === 101)
      //assert(b.leftJoin(a){ (id, a, bOpt) => a + bOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      //val c = VertexSetRDD(a, b.index)
      //assert(b.leftJoin(c){ (id, b, cOpt) => b + cOpt.get }.map(x=> x._2).reduce(_+_) === 0)
      //val d = c.filter(q => ((q._2 % 2) == 0))
      //val e = a.filter(q => ((q._2 % 2) == 0))
      //assert(d.count === e.count)
      //assert(b.zipJoin(c)((id, b, c) => b + c).map(x => x._2).reduce(_+_) === 0)

    }
  }




}



























