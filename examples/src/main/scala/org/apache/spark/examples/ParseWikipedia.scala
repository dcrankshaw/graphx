/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples 

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.mahout.text.wikipedia._

object XMLInputTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "XMLInputTest",
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    // val conf = HBaseConfiguration.create()

    // Other options for configuring scan behavior are available. More information available at 
    // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
    val conf = new Configuration
    conf.set("key.value.separator.in.input.line", " ");
    conf.set("xmlinput.start", "<page>");
    conf.set("xmlinput.end", "</page>");
    val path = args(1)

    val wikiRDD = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable], classOf[Text], conf)
    
    val ct = wikiRDD.count()
    println(ct)

    System.exit(0)
  }


}



// object SplitUkUnion {
//   def main(args: Array[String]) {
//     if (args.length == 0) {
//       System.err.println("Usage: SparkPi <master> [<slices>]")
//       System.exit(1)
//     }
//     val master_ip = args(0)
//     val numParts = 64
//     val master = "spark://" + master_ip + ":7077"
//     val spark = new SparkContext(master, "SplitUkUnion",
//       System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))
// 
//     org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
//     val hdfsBase = "hdfs://" + master_ip + ":9000/"
//     val original = spark.textFile(hdfsBase + "uk0705", numParts)
//     original.saveAsTextFile(hdfsBase + "/uksplits")
// 
//     // val withIndex = original.mapPartitionsWithIndex { (index, lines) => lines.map { l => (index, l)}}
//     // for (i <- (0 until numParts)) {
//     //   println("Processing split: " + i)
//     //   val curRDD = withIndex.filter { case (index, line) => index == i}.map { case (index, line) => line}
//     //   val fname = hdfsBase + "uksplits/split_" + i
//     //   curRDD.saveAsTextFile(fname)
//     // }
// 
// 
// 
//     // val glommed = original.glom()
//     // glommed.foreach { (partArray: Array[String]) =>  {
//     //     val fname = hdfsBase + "uksplits/" + partArray(0)
//     //     spark.parallelize(partArray).saveAsTextFile(fname)
//     //     println(partArray(0))
//     //   }
// 
//     spark.stop()
//   }
// }


// class WikiXMLInputFormat extends FileInputFormat[String, XMLWikiArticle] {
// 
//   def getRecordReader(input: InputSplit,
//                       job: JobConf,
//                       reporter: Reporter): RecordReader[String, XMLWikiArticle] = {
//     return new WikiRecordReader(job, input)
// 
//   }
// }
// 
// class WikiRecordReader
// 
// class XMLWikiArticle(articleText: String) {
// 
// 
// }
// 
// 
// 
// 
// 
// 
// 
// 


