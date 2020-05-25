import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext,DataFrame,SparkSession}
import org.graphframes._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map

import java.io.File
import java.io.PrintWriter
import java.util.Calendar

case class edge_ends(vertex_1:String, vertex_2:String)
case class edges_class(src: String, dst: String)

object HW4_task1 extends Serializable{
  def main(arg: Array[String]): Unit = {
    var spark = new SparkConf().setAppName("test").setMaster("local[*]")
    var sc = new SparkContext(spark)
    sc.setLogLevel("ERROR")

    //Reading the raw data
    //val review_json_path = arg(1)

    //    val input_file = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-2/small1.csv"
//    val input_path = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-4/slides_graph.txt"
    var input_path = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-4/power_input.txt"

    //First command line argument for input file
    input_path = arg(0)

    val input_file = sc.textFile(input_path)
    var edges = input_file.map {line =>val col = line.split(" ")
      edge_ends(col(0), col(1))
    }

    //  Forming a pair RDD
    var edges_final = edges.map(x=> (x.vertex_1,x.vertex_2))

    //    Reading i vertices
    var nodes_i = edges_final.keys.collect()
    //    Reading j vertices
    var nodes_j = edges_final.values.collect()

    //Combining the above two arrays --> Note that this is a set
    var combine_nodes = (nodes_i++nodes_j).toSet

    //Distinct edges
    var edges_distinct =  (edges_final.distinct.collect()).toArray
    var edges_bidirectional = edges_distinct.clone()

    // Had to create an empty Array Buffer
    var edges_add:ArrayBuffer[(String,String)] = ArrayBuffer()

    for(e<-edges_distinct) {
      var reverse_edge = (e._2, e._1)
      if (!edges_distinct.contains(reverse_edge)) {
        // Add the edge
        edges_add += reverse_edge
      }
    }

    // Adding the reverse edges
    edges_bidirectional = edges_bidirectional++edges_add

    // Graph Frames code -
    // Need this for GraphFrames
    val spark_sess: SparkSession =
    SparkSession
      .builder()
      .appName("AppName")
      .config("spark.master", "local")
      .getOrCreate()

    // For some reason we need this
    import spark_sess.implicits._

    //Converting the edged data to a RDD
    val edges_RDD = sc.parallelize(edges_bidirectional).map { case (foo, bar) => edges_class(foo, bar) }

    // Edges and Vertices DataFrames
    var edges_DF = edges_RDD.toDF
    var vertices_DF = edges_DF.select("src").union(edges_DF.select("dst")).distinct().withColumnRenamed("src", "id")

    // Let's create a graph from these edges
    var g = GraphFrame(vertices_DF,edges_DF)

    // Change max iteration here
    var LPA_result = g.labelPropagation.maxIter(5).run()

    println(LPA_result)
    LPA_result.select("id", "label").show(5)

    // GroupBy results
    var result_groupBy = LPA_result.groupBy("label").count()
    var result_groupBy_order = result_groupBy.orderBy("count")
    println("Number of labels:",result_groupBy.count())

    var label_node_list = LPA_result.select("label","id").collect().toList

    // RDD
    var label_node_rdd = sc.parallelize(label_node_list).map(x=>(x(0),x(1)))
    var label_groupBy = label_node_rdd.groupByKey().collect()
    //   making a list of strings
//    var temp_dict = Map[Any,List[String]]()
    var label_groupBy_dict = Map[Any,List[String]]()
    // Had to convert to list of strings!
    for (pair<-label_groupBy){
      // Forming the list of strings
      var temp = List[String]()
      for (v<-pair._2){
        temp::= v.toString
      }
      // Assigning to the key (Group label)
      label_groupBy_dict += (pair._1->temp)
    }

//
//    var len_dict = Map[Any,List[List[Any]]]()
    // Storing lists based on length
    var len_dict = Map[Int,List[List[String]]]()
    for (label <- label_groupBy_dict.keys) {
      var new_key = label_groupBy_dict(label).length
      if (len_dict.contains(new_key)){
        var sorted_list = label_groupBy_dict(label).sorted
        var testLs = len_dict(new_key)++List(sorted_list)
        len_dict+= (new_key->testLs)
      }
      else{
        var sorted_list = label_groupBy_dict(label).sorted
        len_dict+= (new_key->List(sorted_list))

      }
    }

    var OP_file_path = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-1_Scala/src/main/scala/HW4_OP1.txt"
    //Second command line argument for communities
    OP_file_path = arg(1)
//    var OP_file_path = "HW4_OP1.txt"
    //Writing to file
    val writer = new PrintWriter(new File(OP_file_path))
    // For length in ascending order
    for (i<-len_dict.keys.toList.sorted){
      //Print each sorted list
      // Sorting the list of lists
      var sorted_list = len_dict(i).sorted[Iterable[String]]

      for (each_list<-sorted_list){
        var temp = ListBuffer[String]()
          each_list.sorted.mkString(",")
        for (item <- each_list.sorted){
          temp+="'"+item+"'"
        }
        var op = temp.mkString(", ")
//        println(op)
        writer.write(op+"\n")
      }
    }
    writer.close()

}
}
