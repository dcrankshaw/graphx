package org.apache.spark.graph.io

import java.util.{Arrays => JArrays}
import org.apache.spark.graph.impl.EdgePartitionBuilder
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.graph.impl.{EdgePartition, GraphImpl}
import org.apache.spark.util.collection.PrimitiveVector
import org.apache.spark.graph._


object GraphLoader extends Logging {
