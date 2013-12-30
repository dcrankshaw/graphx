package org.apache.spark.graph.io

import java.util.{Arrays => JArrays}
import org.apache.spark.graph.impl.EdgePartitionBuilder
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.graph.impl.{EdgePartition, GraphImpl}
import org.apache.spark.util.collection.PrimitiveVector
import org.apache.spark.graph._


object GraphSaver extends Logging {


  // this saves all the Edge[ED] and (Vid, VD) objects to files, but does not
  // save any metadata (e.g. indexing or EdgePartitions or anything).

  // TODO might have to serialize each object first, then write the serialized entries
  // as byte arrays - RDD.saveAsObjectFile() looks like it might serialize entire partition
  // at once which I don't think is what we want here
  def saveAsObjectFiles[VD,ED]( graph: Graph[VD, ED], path: String) {
    // TODO verify pathnames will work as intended
    graph.edges.saveAsObjectFile(path + "_edges")
    graph.vertices.saveAsObjectFile(path + "_vertices")
  }


  // Note that when loading this graph again, the graph will be split into the
  // same number of partitions
  def saveWithMetadata[VD,ED](graph: Graph[VD,ED], path: String) {

    // TODO not sure what this should look like

  }


}
