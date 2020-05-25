import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext,DataFrame,SparkSession}
import org.graphframes._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.immutable.ListMap
import scala.util.control.Breaks._
import java.io.File
import java.io.PrintWriter
import java.util.Calendar

case class edge_class(vertex_1:String, vertex_2:String)
//case class edges_class(src: String, dst: String)

object task2_HW4_Apr8 extends Serializable {
  def main(arg: Array[String]): Unit = {
    var spark = new SparkConf().setAppName("test").setMaster("local[*]")
    var sc = new SparkContext(spark)
    sc.setLogLevel("ERROR")

    //Reading the raw data
    //val review_json_path = arg(1)

    //    val input_file = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-2/small1.csv"
//        val input_path = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-4/slides_graph.txt"
    var input_path = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-4/power_input.txt"

    //First command line argument for input file
    input_path = arg(0)
    val now = Calendar.getInstance()
    val timestamp1: Long = System.currentTimeMillis / 1000

    val input_file = sc.textFile(input_path)
    var edges = input_file.map { line =>
      val col = line.split(" ")
      edge_class(col(0), col(1))
    }

    //  Forming a pair RDD
    var edges_final = edges.map(x => (x.vertex_1, x.vertex_2))

    //    Reading i vertices
    var nodes_i = edges_final.keys.collect()
    //    Reading j vertices
    var nodes_j = edges_final.values.collect()

    //Combining the above two arrays --> Note that this is a set
    var vertices = (nodes_i ++ nodes_j).toSet.toList
    println(vertices.length)
    //    sys.exit()

    // Creating the tree dictionarty
    var tree_dictionary = Map[String, ListBuffer[String]]()
    for (edge <- edges_final.collect()) {
      // If key is already a part of the map object
      if (tree_dictionary.contains(edge._1)) {
        tree_dictionary(edge._1) += edge._2
        //    #Add the reverse edge too
        if (tree_dictionary.contains(edge._2)) {
          tree_dictionary(edge._2) += edge._1
        }
        else {
          tree_dictionary += (edge._2 -> ListBuffer(edge._1))
        }
      }
      else {
        tree_dictionary += (edge._1 -> ListBuffer(edge._2))
        // Reverse edge
        if (tree_dictionary.contains(edge._2)) {
          tree_dictionary(edge._2) += edge._1
        }
        else {
          tree_dictionary += (edge._2 -> ListBuffer(edge._1))
        }
      }
    }
    var updated_tree = tree_dictionary.clone()
    // Tree dictionary sanity check
    println("Number of keys in tree_dictionary:", tree_dictionary.keys.toList.length)

    // The map partition function
    //    def count_in_a_partition(idx: Int, iter: Iterator[(String, List[String])]): Iterator[(Int, Map[List[String], Int])] = {
    def map_func(x: Tuple2[String, Int]): (String, Map[Tuple2[String, String], Float]) = {
      // To store all the output
      var edge_val_dict = Map[Tuple2[String, String], Float]()

      def findLevels(graph: Map[String, ListBuffer[String]], start_node: String): (Map[String, Int], Map[Int, ListBuffer[String]]) = {
        // dictionary to store depth of each node
        var level = Map[String, Int]()

        // Dictionary to store nodes in each level
        var level_dict = Map[Int, ListBuffer[String]]()
        //
        // dictionary to store whether a node has been visited or not
        var visited = Map[String, Boolean]()
        for (v <- vertices) {
          visited += (v -> false)
        }
        // Queue for BFS
        var queue = ListBuffer[String]()
        queue += start_node

        level(start_node) = 0
        level_dict(0) = ListBuffer(start_node)

        visited(start_node) = true
        //  Until queue is empty!
        while (queue.length != 0) {
          // get the first element of queue
          //            var x = queue(0)
          var x = queue.remove(0)

          //# traverse neighbors of node x
          for (n <- graph(x)) {
            //n is neighbor
            if (!visited(n)) {
              // enqueue n in queue
              queue += n
              // level of n is level of x + 1
              level(n) = level(x) + 1
              if (level_dict.contains(level(x) + 1)) {
                level_dict(level(x) + 1) += n
              }
              else {
                level_dict += ((level(x) + 1) -> ListBuffer(n))
              }
              //Set visited(n)to true!
              visited(n) = true
            }
          }
        }
        return (level, level_dict)
      }


      // Function to find the number of paths to each node from the root

      def num_paths(level_dict: Map[Int, ListBuffer[String]], max_level: Int): Map[String, Int] = {
        var num_path_dict = Map[String, Int]()

        val levels = (0 to max_level).toList
        // Iterating level by level
        for (lvl <- levels) {
          for (node <- level_dict(lvl)) {
            if (lvl == 0) {
              num_path_dict(node) = 1
            }
            else {
              //  Iterate over edges for the node
              for (edge <- updated_tree(node)) {
                if (level_dict(lvl - 1).contains(edge)) {
                  // Setting it to 1 will cause an error!
                  // Here the initial value is set to the number of ways the parent can be reached
                  if (!num_path_dict.contains(node)) {
                    num_path_dict(node) = num_path_dict(edge)
                  }
                  else {
                    if (num_path_dict.contains(edge)) {
                      // Increasing count by the number of ways a parent can be reached
                      num_path_dict(node) += num_path_dict(edge)
                    }
                  }
                }
              }
            }
          }
        }
        return num_path_dict
      }

      // Function to calculate the sum at each node and edge value
      def root_sum(num_path_dict: Map[String, Int], level_dict: Map[Int, ListBuffer[String]], start_node: String): Float = {
        // The first dictionary
        // Every node gets a value 1
        var max_level = level_dict.keys.max
        var node_val = Map[String, Float]()
        for (node <- num_path_dict.keys) {
          node_val(node) = if (node == start_node) 0 else 1

        }
        // Iterate over all the nodes from max_level-1
        var levels = (0 to max_level - 1).toList.reverse
        for (lvl <- levels) {
          for (node <- level_dict(lvl)) {
            // Increase the sum only if there is a path to a node in the level below
            for (node_below <- level_dict(lvl + 1)) {
              if (updated_tree(node).contains(node_below)) {
                //                #################################################
                //                # Might have to multiply with a factor, take a look
                //                #################################################
                //                # int_step[node] = int_step[node] + int_step[node_below]
                //                # The value we add is -
                //                # Number of ways we can reach a child from a parent : num_path_dict[node]
                //                # Contribution the child can make to the edge : node_val[node_below]/num_path_dict[node_below]
                //                # This part is probably confusing!
                var add_val = num_path_dict(node) * (node_val(node_below) / num_path_dict(node_below))
                node_val(node) = node_val(node) + add_val
                // Just sorting the key
                // Getting the key
                //              var key = new Tuple2[String,String] ("1","1")
                var key = if (node < node_below) (node, node_below) else (node_below, node)
                //                if(node<node_below)  {
                //                  key = (node,node_below)
                //                }
                //                else{
                //                  key = (node_below,node)
                //                }
                if (edge_val_dict.contains(key)) {
                  edge_val_dict(key) += add_val
                }
                else {
                  edge_val_dict(key) = add_val
                }
              }
            }
          }

        }

        // return the root node value
        return node_val(start_node)
      }

      // Get the node
      var node = x._1

      // Calling findLevel function
      val op = findLevels(updated_tree, node)
      //Extracting the required values from the output
      var level_dict = op._2
      var level = op._1

      // Getting the max level
      // Obtaining the maximum level
      val max_level = level_dict.keys.max

      // Calling num_paths function
      var num_path_dict = num_paths(level_dict, max_level)

      // Calling root_sum function
      var node_sum = root_sum(num_path_dict, level_dict, node)
      return (node, edge_val_dict)
    }

    var sorted_vertices = vertices.sorted
    var vertices_RDD = sc.parallelize(vertices)
    // Need to make it a pair RDD
    var vertices_RDD_new = vertices_RDD.map(x => (x, 1))

    val op_phase1 = vertices_RDD_new.map(x => map_func(x)).collect()
    //    edge_betweenness_dict = dict(Counter(test_op[i])+Counter(edge_betweenness_dict))
    var edge_betweenness_dict = Map[(String, String), Float]()
    var all_edges = edges_final.collect()
    var edges_sorted = ListBuffer[(String, String)]()

    for (pair <- all_edges) {
      var key = if (pair._1 < pair._2) (pair._1, pair._2) else (pair._2, pair._1)
      edges_sorted += key
    }

    for (i <- edges_sorted) edge_betweenness_dict += (i -> 0)
    for (pair <- op_phase1) {
      //      println("This is the MAP:",pair._1,pair._2)
      for (k <- pair._2.keys.toList) {
        edge_betweenness_dict(k) += pair._2(k)
      }
    }
    for (k <- edge_betweenness_dict.keys.toList) {
      edge_betweenness_dict(k) = edge_betweenness_dict(k) / 2
    }



    var beteweenness_map = Map[Double,ListBuffer[(String, String)]]()

    for (k<-edge_betweenness_dict.keys){
      // Check if with the value exists
      if(beteweenness_map.contains(edge_betweenness_dict(k))){
        beteweenness_map(edge_betweenness_dict(k))+=k
      }
      else{
        beteweenness_map(edge_betweenness_dict(k)) = ListBuffer(k)
      }
    }

    for (k<-beteweenness_map.keys){
      var len_list = beteweenness_map(k).length
      if(len_list!=1){
//        println("Before:",beteweenness_map(k))
        beteweenness_map(k) = beteweenness_map(k).sortBy(x=>(x._1,x._2))
//        println("After:",beteweenness_map(k))
      }
    }
    var OP_file_path = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-1_Scala/src/main/scala/HW4_betweenness.txt"
    //Second command line argument for betweenness
    OP_file_path = arg(1)
    var writer = new PrintWriter(new File(OP_file_path))

    var list_vals = beteweenness_map.keys.toList.sorted.reverse
    for(k<-list_vals) {
      for (pair <- beteweenness_map(k)) {
        var temp = "('" + pair._1 + "','" + pair._2 + "') "
//        println(temp, k)
        writer.write(temp+k.toString+"\n")
      }
    }
    writer.close()

    //Figure out how to write to disc

    println("************************************************************** THIS IS THE MODULARITY PART **************************************************************")

    // Function to form graph dictionary
    def form_graph_dict(edge_updated: ListBuffer[(String, String)]): Map[String, ListBuffer[String]] = {
      var tree_dictionary_updated = Map[String, ListBuffer[String]]()
      for (i <- edge_updated) {
        if (tree_dictionary_updated.contains(i._1)) {
          tree_dictionary_updated(i._1) += i._2
          // Add the reverse edge too
          if (tree_dictionary_updated.contains(i._2)) {
            tree_dictionary_updated(i._2) += i._1
          }
          else {
            tree_dictionary_updated(i._2) = ListBuffer(i._1)
          }
        }
        else {
          tree_dictionary_updated(i._1) = ListBuffer(i._2)
          // Add the reverse edge too
          if (tree_dictionary_updated.contains(i._2)) {
            tree_dictionary_updated(i._2) += i._1
          }
          else {
            tree_dictionary_updated(i._2) = ListBuffer(i._1)
          }
        }
      }
      return tree_dictionary_updated
    }

    def BFS(graph: Map[String, ListBuffer[String]], x: String): ListBuffer[String] = {
      // dictionary to store whether a node has been visited or not
      var visited = Map[String, Boolean]()
      for (v <- vertices) {
        visited += (v -> false)
      }
      // Queue for BFS
      var queue = ListBuffer[String]()
      queue += x
      //For OP
      var temp_list = ListBuffer[String]()
      visited(x) = true
      //  Until queue is empty!
      while (queue.length != 0) {
        // get the first element of queue
        //            var x = queue(0)

        var x = queue.remove(0)
        temp_list += x
        //# traverse neighbors of node x
        for (n <- graph(x)) {
          //n is neighbor
          if (!visited(n)) {
            // enqueue n in queue
            queue += n
            //Set visited(n)to true!
            visited(n) = true
          }
        }
      }
      return temp_list
    }

    // Function which will return each component
    def connected_components(G: Map[String, ListBuffer[String]], vertices: List[String]): ListBuffer[ListBuffer[String]] = {
      var seen = Set[String]()
      var c = ListBuffer[ListBuffer[String]]()
      for (v <- G.keys.toList) {
        if (!seen(v)) {
          var op = (BFS(G, v))

          for (i<-op.toList){
            seen+=i
          }
          c+=op
        }
      }
      return c
    }

    // Function to calculate modularity
    def calc_modularity(component:ListBuffer[String], graph:Map[String, ListBuffer[String]]):Double={
      var sums = 0.0
      var num_edges = edges.count()
      var num_edges_double = 2 * num_edges

      for (node_1<-component){
        for (node_2<-component){
          // 1 if an edge exists between the two nodes
          var a_ij:Double = if(tree_dictionary(node_2).contains(node_1)) 1.0 else 0.0
          var k_i:Double = (tree_dictionary(node_1)).length
          var k_j:Double = (tree_dictionary(node_2)).length
          var degrees_prod = k_i*k_j

          var val_b:Double = (degrees_prod / num_edges_double)
          var val_change:Double = (a_ij - val_b)
        sums += val_change
      }
}
    return sums
    }


    // Function to calculate modularity
    def calc_modularity_2(component:ListBuffer[String], graph:Map[String, ListBuffer[String]]):Double={
      var sums = 0.0
      var num_edges = edges.count()
      var num_edges_double = 2 * num_edges

      for (node_1<-component){
        for (node_2<-component){
          // 1 if an edge exists between the two nodes
          var a_ij:Double = if(tree_dictionary(node_2).contains(node_1)) 1.0 else 0.0
          var k_i:Double = (tree_dictionary(node_1)).length
          var k_j:Double = (tree_dictionary(node_2)).length
          var degrees_prod = k_i*k_j

          var val_b:Double = (degrees_prod / num_edges_double)
//          var val_change:Double = (a_ij - val_b)
          sums += (a_ij - val_b)
        }
      }
      return sums
    }
    println("This is updated tree:",updated_tree)

    var num_edges = edges.count()

    // Updated tree!
    updated_tree = tree_dictionary.clone()
    // To store edge modularity values
//    modularity_values_dict = {}

    // Dummy max value
    var max_modularity = -1.0

    //Copy of lists to manipulate
    var edge_updated = edges_sorted

    var max_mod_components = ListBuffer[ListBuffer[String]]()
    var best_graph = Map[String, ListBuffer[String]]()
    var components = ListBuffer[ListBuffer[String]]()
    var components_length = 0
    var count = 0
    var last_mod = 1.0
      breakable {
        while (edge_updated.length != 0) {
          //    while (components_length!=sorted_vertices.length){

          // Getting the mapping value!
          val op_phase1 = vertices_RDD_new.map(x => map_func(x)).collect()

          var edge_betweenness_dict = Map[(String, String), Float]()


          for (i <- edges_sorted) edge_betweenness_dict += (i -> 0)
          for (pair <- op_phase1) {
            //      println("This is the MAP:",pair._1,pair._2)
            for (k <- pair._2.keys.toList) {
              edge_betweenness_dict(k) += pair._2(k)
            }
          }
          for (k <- edge_betweenness_dict.keys.toList) {
            edge_betweenness_dict(k) = edge_betweenness_dict(k) / 2
          }
          // COnverting to List Map
          val res = ListMap(edge_betweenness_dict.toSeq.sortWith(_._2 > _._2): _*)

          // Obtaining edge with Max Betweenness
          var max_edge = res.head._1

          // Delete that edge
          edge_updated -= max_edge

          // Updating the tree
          updated_tree = form_graph_dict(edge_updated)

          for (v <- sorted_vertices) {
            if (!updated_tree.contains(v)) {
              updated_tree(v) = ListBuffer[String]()
            }
          }

          // Obtaining the list of components
          var components = connected_components(updated_tree, sorted_vertices)
//          println("Number of components:", components.length)
          components_length = components.length


          var sum_modularity = 0.0
          for (component <- components) {
            sum_modularity += calc_modularity_2(component, updated_tree)
          }
          var modularity = sum_modularity / (2 * num_edges)
//          println("Modularity:", modularity)

//          var modularity_values_dict = Map[Int, ListBuffer[Float]]()

          if (modularity >= max_modularity) {
            max_modularity = modularity
            max_mod_components = components
            best_graph = updated_tree.clone()
          }

          if (max_modularity - modularity >= 0.6) break

//          println("****************************************************************************************************************************")
        }
      }
      println("************************************************************** BEST CASE **************************************************************")
      println("Best case:")
      println("Modularity is:",max_modularity)
      println("Components are:",max_mod_components)
      println("Number of components:",max_mod_components.length)
      println("This is the graph:",best_graph)
      println("************************************************************** BEST CASE **************************************************************")

      val timestamp2: Long = System.currentTimeMillis / 1000

      println("Time taken:",timestamp2-timestamp1)

    var sorted_components = ListBuffer[ListBuffer[String]]()

    for(comp<-max_mod_components){
      sorted_components+= comp.sorted
    }

    var map_op = Map[Int,ListBuffer[ListBuffer[String]]]()

    for (comp<-sorted_components){
      var len = comp.length
      if (map_op.contains(len)){
        map_op(len)+=comp
      }
      else{
        map_op(len) = ListBuffer(comp)
      }
    }
    OP_file_path = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-1_Scala/src/main/scala/HW4_communities.txt"
    //Third command line argument for communities
    OP_file_path = arg(2)
    //Writing to file
    // Iterate over all the keys
    for (key<-map_op.keys.toList.sorted){
      var len_list = map_op(key).length
      //Ignore if length of list = 1
      if (len_list!=1){
        // Store the first element of each list
        var temp_elem = ListBuffer[String]()
      for (list <- map_op(key)){
        temp_elem+=list.head
      }
        // Sort by the first element
        temp_elem = temp_elem.sorted

        var temp_list_2 = ListBuffer[ListBuffer[String]]()
        // Iterate over all the sorted elements
        for (i<-temp_elem){
          for (list <- map_op(key)){
            // If the list contains sorted elements, append the list
            if(list.contains(i)){
              temp_list_2+=list
            }
          }
        }
//        println("This is the list after changing:",temp_list_2)
        map_op(key) = temp_list_2
      }
    }

    writer = new PrintWriter(new File(OP_file_path))
    for (key<-map_op.keys.toList.sorted){
      for (list <- map_op(key)){

          var temp = ListBuffer[String]()
          for (item <- list){
            temp+="'"+item+"'"
          }
          var op = temp.mkString(", ")
//          println(op)
          writer.write(op+"\n")
      }
    }
    writer.close()
  }
}