/**
 * Copyright 2014 MIT Lincoln Laboratory, Massachusetts Institute of Technology
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uiuc.topksubgraph;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.jgrapht.util.FibonacciHeap;
import org.jgrapht.util.FibonacciHeapNode;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

/**
 * Test-class for both ingest and sub-graph query for bitcoin_small example
 *
 * @author Charlie Dagli (dagli@ll.mit.edu)
 */
public class IngestAndQuery {

  private static Logger logger = Logger.getLogger(IngestAndQuery.class);

  /**
   * @param args
   */
  public static void main(String[] args) throws Throwable {
    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();

    logger.info("Computing Indices...");
    computeIndices();

    logger.info("Executing Query...");
    executeQuery();


  }

  /**
   * @throws FileNotFoundException
   * @throws IOException
   * @throws Throwable
   * @throws NumberFormatException
   */
  public static void executeQuery() throws FileNotFoundException,
      IOException, Throwable, NumberFormatException {

    QueryExecutor executor = new QueryExecutor();

    QueryExecutor.baseDir = "data/bitcoin/";
    QueryExecutor.graphFile = "graphs/bitcoin_small_transactions.txt";
    QueryExecutor.typesFile = "graphs/bitcoin_small_types.txt";
    QueryExecutor.queryFile = "queries/queryGraph.FanOut.txt";
    QueryExecutor.queryTypesFile = "queries/queryTypes.FanOut.txt";
    //QueryExecutor.spathFile = "indices/bitcoin_small_transactions.2.spath";
    QueryExecutor.topologyFile = "indices/bitcoin_small_transactions.2.topology";
    QueryExecutor.spdFile = "indices/bitcoin_small_transactions.2.spd";
    QueryExecutor.resultDir = "results";

    windowsPathnamePortabilityCheck();

    //String pattern = Pattern.quote(System.getProperty("file.separator"));
    String pattern = "/";
    String[] splitGraphFile = QueryExecutor.graphFile.split(pattern);
    QueryExecutor.graphFileBasename = splitGraphFile[splitGraphFile.length - 1];

    QueryExecutor.k0 = 2;
    QueryExecutor.topK = 1000;

    /**
     * Load-in types, and count how many there are
     */
    // Load-in types
    HashMap<Integer, Integer> node2Type;
    node2Type = loadTypesFile(QueryExecutor.baseDir, QueryExecutor.typesFile);
    executor.setNode2Type(node2Type);

    // Count types
    executor.computeTotalTypes();
    int totalTypes = executor.getTotalTypes();
    logger.debug("Computed number of types: " + totalTypes);

    logger.info("Loaded-in types...");

    executor.loadGraphNodesType();    //compute ordering
    executor.loadGraphSignatures();    //topology
    executor.loadEdgeLists();      //sorted edge lists
    executor.loadSPDIndex();      //spd index

    //Make resultDir if neccessary
    File directory = new File(QueryExecutor.baseDir + QueryExecutor.resultDir);
    if (!directory.exists() && !directory.mkdirs())
      throw new IOException("Could not create directory: " + QueryExecutor.baseDir + QueryExecutor.resultDir);

    //set system out to out-file...
    System.setOut(new PrintStream(new File(QueryExecutor.baseDir + QueryExecutor.resultDir + "/QBSQueryExecutorV2.topK=" + QueryExecutor.topK + "_K0=" + QueryExecutor.k0 + "_" + QueryExecutor.graphFileBasename.split("\\.")[0] + "_" + QueryExecutor.queryFile.split("/")[1])));

    /**
     * Read-in and setup query
     */
    int isClique = executor.loadQuery();

    executor.getQuerySignatures(); //fills in querySign

    /**
     * NS Containment Check and Candidate Generation
     */
    long time1 = new Date().getTime();

    int prunedCandidateFiltering = executor.generateCandidates();
    if (prunedCandidateFiltering < 0) {
      return;
    }

    long timeA = new Date().getTime();
    System.out.println("Candidate Generation Time: " + (timeA - time1));


    /**
     * Populate all required HashMaps relating edges to edge-types
     */
    // compute edge types for all edges in query
    HashSet<String> queryEdgeTypes = executor.computeQueryEdgeTypes();

    //compute queryEdgetoIndex
    executor.computeQueryEdge2Index();

    //compute queryEdgeType2Edges
    HashMap<String, ArrayList<String>> queryEdgeType2Edges = executor.computeQueryEdgeType2Edges();


    //Maintain pointers and topk heap
    executor.computePointers(queryEdgeTypes, queryEdgeType2Edges);

    /**
     * The secret sauce... Execute the query...
     */
    executor.executeQuery(queryEdgeType2Edges, isClique, prunedCandidateFiltering);

    long time2 = new Date().getTime();
    System.out.println("Overall Time: " + (time2 - time1));


    //FibonacciHeap<ArrayList<String>> queryResults = executor.getHeap();
    executor.printHeap();

  }

  /**
   * If we're on Windows, convert file-separators to "/"
   * (this should probably be in an utilities package at some point)
   */
  private static void windowsPathnamePortabilityCheck() {
    String osName = System.getProperty("os.name");

    if (osName.startsWith("Windows")) {
      String separator = Pattern.quote(System.getProperty("file.separator"));

      QueryExecutor.baseDir = QueryExecutor.baseDir.replace(separator, "/");
      QueryExecutor.graphFile = QueryExecutor.graphFile.replace(separator, "/");
      QueryExecutor.typesFile = QueryExecutor.typesFile.replace(separator, "/");
      QueryExecutor.queryFile = QueryExecutor.queryFile.replace(separator, "/");
      QueryExecutor.queryTypesFile = QueryExecutor.queryTypesFile.replace(separator, "/");
      //QueryExecutor.spathFile = QueryExecutor.spathFile.replace(separator, "/");
      QueryExecutor.topologyFile = QueryExecutor.topologyFile.replace(separator, "/");
      QueryExecutor.spdFile = QueryExecutor.spdFile.replace(separator, "/");
      QueryExecutor.resultDir = QueryExecutor.resultDir.replace(separator, "/");
    }
  }

  /**
   * To perform the graph indexing process...
   */
  private static void computeIndices() throws Throwable {
    MultipleIndexConstructor.baseDir = "data/bitcoin/graphs/";
    MultipleIndexConstructor.outDir = "data/bitcoin/indices/";
    MultipleIndexConstructor.graphFile = "bitcoin_small_transactions.txt";
    MultipleIndexConstructor.typesFile = "bitcoin_small_types.txt";
    MultipleIndexConstructor.D = 2;

    long time1 = new Date().getTime();

    // Load-in graph
    Graph g = new Graph();
    g.loadGraph(new File(MultipleIndexConstructor.baseDir, MultipleIndexConstructor.graphFile));
    MultipleIndexConstructor.setGraph(g);
    logger.info("Loaded graph file...");

    // Load-in types
    //MultipleIndexConstructionTest indexTester = new MultipleIndexConstructionTest();
    HashMap<Integer, Integer> node2Type;
    node2Type = loadTypesFile(MultipleIndexConstructor.baseDir, MultipleIndexConstructor.typesFile);
    MultipleIndexConstructor.setNode2Type(node2Type);
    logger.info("Loaded types file...");

    //compute totalTypes
    MultipleIndexConstructor.totalTypes = 0;
    MultipleIndexConstructor.computeTotalTypes();
    logger.debug("Computed number of types: " + MultipleIndexConstructor.totalTypes);

    logger.info("Building Sorted Edge List Index...");

    // Create Typed Edges
    MultipleIndexConstructor.createTypedEdges();

    // Load and Sort Edges from Graph
    //MultipleIndexConstructor.loadAndSortEdges();
    MultipleIndexConstructor.populateSortedEdgeLists(g);

    //save the sorted edge lists
    try {
      MultipleIndexConstructor.saveSortedEdgeList();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    //hash map for all possible "edge-type" paths: i.e. doubles,triples,...D-tuples
    //this gets you the "official" ordering
    logger.info("Computing Edge-Type Path Ordering...");
    MultipleIndexConstructor.computeEdgeTypePathOrdering();

    logger.info("Computing SPD, Topology and SPath Indices...");

    try {
      MultipleIndexConstructor.computeIndices(g);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    long time2 = new Date().getTime();
    logger.info("Time:" + (time2 - time1));
  }

  /**
   * Load types file and return HashMap
   *
   * @param typesFile
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static HashMap<Integer, Integer> loadTypesFile(String baseDir, String typesFile)
      throws FileNotFoundException, IOException {

    HashMap<Integer, Integer> node2Type = new HashMap<Integer, Integer>();

    //load types file
    BufferedReader in = new BufferedReader(new FileReader(new File(baseDir, typesFile)));
    String str = "";
    while ((str = in.readLine()) != null) {
      String tokens[] = str.split("\\t");
      int node = Integer.parseInt(tokens[0]);
      int type = Integer.parseInt(tokens[1]);
      node2Type.put(node, type);
    }
    in.close();

    return node2Type;
  }

  /**
   *
   */
  public static void printHeap(FibonacciHeap<ArrayList<String>> heap) {
    System.out.println("============================================================================");
    while (!heap.isEmpty()) {
      FibonacciHeapNode<ArrayList<String>> fhn = heap.removeMin();
      ArrayList<String> list = fhn.getData();
      for (int i = 0; i < list.size(); i++)
        System.out.print(list.get(i) + "\t");
      System.out.print(fhn.getKey());
      System.out.println();
    }
    System.out.println("============================================================================");
//		System.exit(0);
  }

}
