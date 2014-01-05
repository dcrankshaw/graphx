
package org.apache.spark.graph.algorithms

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graph._
import org.apache.spark.graph.util.GraphGenerators
import org.apache.spark.rdd._
import scala.collection.mutable


class TopicModelingSuite extends FunSuite with LocalSparkContext {

  test("makeTokens") {
    withSpark { sc =>
      val docs = List("hello World foo", "Foo bar graphs", "Graphs are fun")
      val uniqueWords = Set("hello", "world", "foo", "bar", "graphs", "are", "fun")
      val tokens = LDA.makeTokens(sc.parallelize(docs))
      assert(tokens.count === 9)
    }
  }

  test("makeDictionary") {
    withSpark { sc =>
      val docs = List("hello World foo", "Foo bar graphs", "Graphs are fun")
      val uniqueWords = Set("hello", "world", "foo", "bar", "graphs", "are", "fun")
      val dict = LDA.makeDictionary(sc.parallelize(docs))
      assert(dict.size === 7)
      assert(uniqueWords === dict.toList.map(_._2).toSet)
    }
  }

}
