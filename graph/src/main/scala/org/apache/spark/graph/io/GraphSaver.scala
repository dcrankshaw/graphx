package org.apache.spark.graph.io

import java.util.{Arrays => JArrays}
import org.apache.spark.graph.impl.EdgePartitionBuilder
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.graph.impl.{EdgePartition, GraphImpl}
import org.apache.spark.util.collection.PrimitiveVector
import org.apache.spark.graph._


object GraphSaver extends Logging {


  // this saves all the Edge[ED] and (Vid, VD) objects to files, but does not
  // save any metadata (e.g. indexing or EdgePartitions or anything).
  def saveAsObjectFiles[VD: ClassManifest, ED: ClassManifest](graph: Graph[VD, ED], vpath: String, epath:String) {
    graph.edges.map(_.clone).saveAsObjectFile(epath)
    graph.vertices.saveAsObjectFile(vpath)
  }


  // Note that when loading this graph again, the graph will be split into the
  // same number of partitions
  // def saveWithMetadata[VD,ED](graph: Graph[VD,ED], path: String) {

  //   // TODO not sure what this should look like

  // }


}
