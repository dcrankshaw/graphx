package org.apache.spark.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.util.ClosureCleaner
import org.apache.spark.SparkException


/**
 * `GraphOps` contains additional functionality (syntatic sugar) for
 * the graph type and is implicitly constructed for each Graph object.
 * All operations in `GraphOps` are expressed in terms of the
 * efficient GraphX API.
 *
 * @tparam VD the vertex attribute type
 * @tparam ED the edge attribute type
 *
 */
class GraphOps[VD: ClassManifest, ED: ClassManifest](graph: Graph[VD, ED]) {

  /**
   * Compute the number of edges in the graph.
   */
  lazy val numEdges: Long = graph.edges.count()


  /**
   * Compute the number of vertices in the graph.
   */
  lazy val numVertices: Long = graph.vertices.count()


  /**
   * Compute the in-degree of each vertex in the Graph returning an
   * RDD.
   * @note Vertices with no in edges are not returned in the resulting RDD.
   */
  lazy val inDegrees: VertexSetRDD[Int] = degreesRDD(EdgeDirection.In)


  /**
   * Compute the out-degree of each vertex in the Graph returning an RDD.
   * @note Vertices with no out edges are not returned in the resulting RDD.
   */
  lazy val outDegrees: VertexSetRDD[Int] = degreesRDD(EdgeDirection.Out)


  /**
   * Compute the degrees of each vertex in the Graph returning an RDD.
   * @note Vertices with no edges are not returned in the resulting
   * RDD.
   */
  lazy val degrees: VertexSetRDD[Int] = degreesRDD(EdgeDirection.Both)


  /**
   * Compute the neighboring vertex degrees.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertex attributes.
   */
  private def degreesRDD(edgeDirection: EdgeDirection): VertexSetRDD[Int] = {
    if (edgeDirection == EdgeDirection.In) {
      graph.mapReduceTriplets(et => Iterator((et.dstId,1)), _ + _)
    } else if (edgeDirection == EdgeDirection.Out) {
      graph.mapReduceTriplets(et => Iterator((et.srcId,1)), _ + _)
    } else { // EdgeDirection.both
      graph.mapReduceTriplets(et => Iterator((et.srcId,1), (et.dstId,1)), _ + _)
    }
  }


  /**
   * This function is used to compute a statistic for the neighborhood
   * of each vertex and returns a value for all vertices (including
   * those without neighbors).
   *
   * @note Because the a default value is provided all vertices will
   * have a corresponding entry in the returned RDD.
   *
   * @param mapFunc the function applied to each edge adjacent to each
   * vertex.  The mapFunc can optionally return None in which case it
   * does not contribute to the final sum.
   * @param reduceFunc the function used to merge the results of each
   * map operation.
   * @param default the default value to use for each vertex if it has
   * no neighbors or the map function repeatedly evaluates to none
   * @param direction the direction of edges to consider (e.g., In,
   * Out, Both).
   * @tparam VD2 The returned type of the aggregation operation.
   *
   * @return A Spark.RDD containing tuples of vertex identifiers and
   * their resulting value.  There will be exactly one entry for ever
   * vertex in the original graph.
   *
   * @example We can use this function to compute the average follower
   * age for each user
   *
   * {{{
   * val graph: Graph[Int,Int] = loadGraph()
   * val averageFollowerAge: RDD[(Int, Int)] =
   *   graph.aggregateNeighbors[(Int,Double)](
   *     (vid, edge) => (edge.otherVertex(vid).data, 1),
   *     (a, b) => (a._1 + b._1, a._2 + b._2),
   *     -1,
   *     EdgeDirection.In)
   *     .mapValues{ case (sum,followers) => sum.toDouble / followers}
   * }}}
   *
   * @todo Should this return a graph with the new vertex values?
   *
   */
  def aggregateNeighbors[A: ClassManifest](
      mapFunc: (Vid, EdgeTriplet[VD, ED]) => Option[A],
      reduceFunc: (A, A) => A,
      dir: EdgeDirection)
    : VertexSetRDD[A] = {

    ClosureCleaner.clean(mapFunc)
    ClosureCleaner.clean(reduceFunc)

    // Define a new map function over edge triplets
    val mf = (et: EdgeTriplet[VD,ED]) => {
      // Compute the message to the dst vertex
      val dst =
        if (dir == EdgeDirection.In || dir == EdgeDirection.Both) {
          mapFunc(et.dstId, et)
        } else { Option.empty[A] }
      // Compute the message to the source vertex
      val src =
        if (dir == EdgeDirection.Out || dir == EdgeDirection.Both) {
          mapFunc(et.srcId, et)
        } else { Option.empty[A] }
      // construct the return array
      (src, dst) match {
        case (None, None) => Iterator.empty
        case (Some(srcA),None) => Iterator((et.srcId, srcA))
        case (None, Some(dstA)) => Iterator((et.dstId, dstA))
        case (Some(srcA), Some(dstA)) => Iterator((et.srcId, srcA), (et.dstId, dstA))
      }
    }

    ClosureCleaner.clean(mf)
    graph.mapReduceTriplets(mf, reduceFunc)
  } // end of aggregateNeighbors


  /**
   * Return the Ids of the neighboring vertices.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the vertex set of neighboring ids for each vertex.
   */
  def collectNeighborIds(edgeDirection: EdgeDirection) :
    VertexSetRDD[Array[Vid]] = {
    val nbrs =
      if (edgeDirection == EdgeDirection.Both) {
        graph.mapReduceTriplets[Array[Vid]](
          mapFunc = et => Iterator((et.srcId, Array(et.dstId)), (et.dstId, Array(et.srcId))),
          reduceFunc = _ ++ _
        )
      } else if (edgeDirection == EdgeDirection.Out) {
        graph.mapReduceTriplets[Array[Vid]](
          mapFunc = et => Iterator((et.srcId, Array(et.dstId))),
          reduceFunc = _ ++ _)
      } else if (edgeDirection == EdgeDirection.In) {
        graph.mapReduceTriplets[Array[Vid]](
          mapFunc = et => Iterator((et.dstId, Array(et.srcId))),
          reduceFunc = _ ++ _)
      } else {
        throw new SparkException("It doesn't make sense to collect neighbor ids without a direction.")
      }
    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) => nbrsOpt.getOrElse(Array.empty[Vid]) }
  } // end of collectNeighborIds


  /**
   * Collect the neighbor vertex attributes for each vertex.
   *
   * @note This function could be highly inefficient on power-law
   * graphs where high degree vertices may force a large ammount of
   * information to be collected to a single location.
   *
   * @param edgeDirection the direction along which to collect
   * neighboring vertices
   *
   * @return the vertex set of neighboring vertex attributes for each
   * vertex.
   */
  def collectNeighbors(edgeDirection: EdgeDirection) :
    VertexSetRDD[ Array[(Vid, VD)] ] = {
    val nbrs = graph.aggregateNeighbors[Array[(Vid,VD)]](
      (vid, edge) =>
        Some(Array( (edge.otherVertexId(vid), edge.otherVertexAttr(vid)) )),
      (a, b) => a ++ b,
      edgeDirection)

    graph.vertices.leftZipJoin(nbrs) { (vid, vdata, nbrsOpt) => nbrsOpt.getOrElse(Array.empty[(Vid, VD)]) }
  } // end of collectNeighbor


  /**
   * Join the vertices with an RDD and then apply a function from the
   * the vertex and RDD entry to a new vertex value.  The input table
   * should contain at most one entry for each vertex.  If no entry is
   * provided the map function is skipped and the old value is used.
   *
   * @tparam U the type of entry in the table of updates
   * @param table the table to join with the vertices in the graph.
   * The table should contain at most one entry for each vertex.
   * @param mapFunc the function used to compute the new vertex
   * values.  The map function is invoked only for vertices with a
   * corresponding entry in the table otherwise the old vertex value
   * is used.
   *
   * @note for small tables this function can be much more efficient
   * than leftJoinVertices
   *
   * @example This function is used to update the vertices with new
   * values based on external data.  For example we could add the out
   * degree to each vertex record
   *
   * {{{
   * val rawGraph: Graph[Int,()] = Graph.textFile("webgraph")
   *   .mapVertices(v => 0)
   * val outDeg: RDD[(Int, Int)] = rawGraph.outDegrees()
   * val graph = rawGraph.leftJoinVertices[Int,Int](outDeg,
   *   (v, deg) => deg )
   * }}}
   *
   */
  def joinVertices[U: ClassManifest](table: RDD[(Vid, U)])(mapFunc: (Vid, VD, U) => VD)
    : Graph[VD, ED] = {
    ClosureCleaner.clean(mapFunc)
    val uf = (id: Vid, data: VD, o: Option[U]) => {
      o match {
        case Some(u) => mapFunc(id, data, u)
        case None => data
      }
    }
    ClosureCleaner.clean(uf)
    graph.outerJoinVertices(table)(uf)
  }


  /*
   * When doing edge contractions, after identifying edges to be contracted
   * all edges in the graph fall into 2 disjoint sets:
   * 
   *    A) Edges to be contracted
   *    B) Edges not to be contracted
   *
   * Based on the defined semantics of the function what to do with edges in
   * set A is well defined. We create a (probably unconnected) subgraph G'
   * where E' == A. We then run (undirected?) connected components on G'.
   * Each cc will be a single vertex in the resulting coarsened graph.
   * Once we have identified the cc's we can merge all of the edges until
   * we have a single vertex. This can either be done in a fold like way (with
   * an accumulator) or in a reduce like way. With reduce, we could merge edges
   * in a k-ary tree for better parallelism (this may or may not be worth it).
   * I'm not sure how we could parallelize fold. Finally, we need to figure out
   * how to join the new vertices back into G, which brings us to edges in set B.
   * B can be further divided into 4 categories:
   *
   *    B.1) unchanged: Edges e such that no edges in the neighborhood of either their
   *         src or dst vertex will be contracted. No action needs to be taken with
   *         these edges.
   *    B.2) join edges: 1 new vertex: Edges e such that EITHER their src or dst vertex
   *         is the src or dst vertex of at least one edge that will be contracted.
   *         After contracting edges, this edge needs to figure out which connected
   *         component its merged vertex src or dst ended up in and form a new edge
   *         with this new contracted vertex. Additionally complicating things here
   *         is that vertex u could be src of 2 edges (u,v, uv.data) and (u,w, uw.data) where both
   *         v and w got contracted into the same cc. This means that in the resulting
   *         graph where v and w got contracted into vertex x, there are now 2 edges
   *         between u and x: (u,x, uv.data) and (u,x, uw.data), so these edges need to be
   *         merged.
   *    B.3) join edges: 2 new vertices: Edges e such that BOTH their src and dst vertices
   *         are the src or dst vertices of at least one edge that will be contracted, but e.src
   *         and e.dst end up in two different cc's. This edge needs to figure out which connected
   *         component each vertex ended up in. We also may have multiple edges between these two
   *         new vertices that would need to be merged.
   *   B.4) self-loops: Edges e such that they have neighbors
   *        through both their src and dst vertices that will be contracted in such a way
   *        that their src and dst end up in the same connected component.
   *
   * Both B.2 and B.3 need to handle elimination of parallel edges. B.4 needs to eliminate
   * self-loops.
   *    
   *
   *
   * In terms of dealing with parallel edges, I think it is okay to return a hypergraph.
   * If the user wants to remove parallel edges, they can run groupEdges afterwards.
   *
   *
   *
   */


  // TODO(dcrankshaw) worry about efficiency - specifically indexes and shuffling 
  def contractEdges(epred: EdgeTriplet[VD,ED] => Boolean,
    contractF: EdgeTriplet[VD,ED] => VD,
    //mergeF: (VD, VD) => VD): Graph[VD:ED] = {
    mergeF: (VD, VD) => VD) = {


    ClosureCleaner.clean(epred)
    ClosureCleaner.clean(contractF)
    ClosureCleaner.clean(mergeF)
    // TODO(dcrankshaw) ClosureClean.clean() funcs

    // subgraph gets re-indexed so worrying about indices might not be a huge deal
    val edgesToContract = graph.subgraph(epred)
    // this should return the disjoint set of edges in set B
    val uncontractedEdges = graph.subgraph( {case(et) =>  !epred(et)})

    // Connected components graph has Vid as vertex data
    val ccGraph: Graph[Vid,ED] = Analytics.connectedComponents(edgesToContract)
    //val ccGraph: Graph[Vid,ED] = Analytics.connectedComponents(graph)



    // join vertex data back with connected component data
    val ccVerticesWithData = edgesToContract.vertices.zipJoin(ccGraph.vertices)((id, data, cc) => (cc,data))
     ////TODO(dcrankshaw) might be able to make this Graph.apply() more efficient by reusing index
    val ccWithDataGraph = Graph(ccVerticesWithData, edgesToContract.edges)
    val triplets: RDD[EdgeTriplet[(Vid,VD),ED]] = ccWithDataGraph.triplets

    def contractTriplet(edge: EdgeTriplet[(Vid,VD),ED]): VD = {
      val et = new EdgeTriplet[VD, ED]
      et.srcId = edge.srcId
      et.srcAttr = edge.srcAttr._2
      et.dstId = edge.dstId
      et.dstAttr = edge.dstAttr._2
      et.attr = edge.attr
      contractF(et)
    }

    // What I really want to do here is
    // a) go from vertices and edges to EdgeTriplets
    // b) the equivalent of
    //    SELECT contract(t) FROM triplets GROUP BY t.cc
    //  where contract is an aggregation function

    // RDD[(Vid, Seq[EdgeTriplet[VD,ED]])]
    // this contracts every connected component into a single (Vid, VD)
    val contractedVertices1: RDD[(Vid, Seq[EdgeTriplet[(Vid,VD),ED]])]  = triplets.groupBy((t => t.srcAttr._1))
    val contractedVertices2: RDD[(Vid, VD)] =
      contractedVertices1.map { case (ccID, verts) => {
        val reducedVertex: VD = verts.map(contractTriplet).reduce(mergeF)
        (ccID, reducedVertex)
      }
    }



    // Now join vertices
    // Maybe the best way to do this is a hash join??????




  }



} // end of GraphOps
































