package org.apache.spark.graph.io

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.rdd._

import java.io.{FileWriter, PrintWriter, File}

// import scala.io.Source

import com.google.common.io.Files
import org.apache.hadoop.io._
import org.apache.hadoop.io.compress.{DefaultCodec, CompressionCodec, GzipCodec}

class IOSuite extends FunSuite with LocalSparkContext {


  def starGraph(sc: SparkContext, n: Int): Graph[String, Int] = {
    Graph.fromEdgeTuples(sc.parallelize((1 to n).map(x => (0: Vid, x: Vid)), 3), "v")
  }

  test("save as object files") {
    withSpark { sc =>
      val graph = starGraph(sc, 10)
      val tempDir = Files.createTempDir()
      val vpath = new File(tempDir, "output_vertices").getAbsolutePath
      val epath = new File(tempDir, "output_edges").getAbsolutePath
      GraphSaver.saveAsObjectFiles(graph, vpath, epath)
      val resultGraph = GraphLoader.loadFromObjectFiles(sc, vpath, epath)
      assert(resultGraph.vertices.collect.toSet === graph.vertices.collect.toSet)
      assert(resultGraph.edges.collect.toSet === graph.edges.collect.toSet)
    }
  }


}
