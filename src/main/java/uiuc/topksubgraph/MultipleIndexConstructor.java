/**
 * Copyright 2014-2016 MIT Lincoln Laboratory, Massachusetts Institute of Technology
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

import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesBase;
import mitll.xdata.db.DBConnection;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Collapses SortedEdgeListConstructor and SPDAndTopologyAndSPathIndexConstructor2 classes into one
 *
 * @author Charlie Dagli (dagli@ll.mit.edu)
 *         MIT Lincoln Laboratory
 *         <p>
 *         extending code from @author Manish Gupta (gupta58@illinois.edu)
 */
@SuppressWarnings("CanBeFinal")
public class MultipleIndexConstructor {
  private static final Logger logger = Logger.getLogger(MultipleIndexConstructor.class);
  private static final int NOTICE_MOD = 5000;

  public static String baseDir = "data/bitcoin/graphs";
  public static String outDir = "data/bitcoin/indices";
  public static String graphFile = "";
  public static String typesFile = "";
  public static int D = 2;

  private static Map<Integer, Integer> node2Type = new HashMap<>();
  private static final Map<String, List<Edge>> sortedEdgeLists = new HashMap<>();
  private static final Map<Integer, List<String>> ordering = new HashMap<>();
  private static final DecimalFormat twoDForm = new DecimalFormat("#.####");
  private static Graph graph = new Graph();

  public static int totalTypes = 0;
  private static final boolean DEBUG = false;

  /**
   * @param args
   */
  public static void main(String[] args) throws Throwable {

    // Set up a simple configuration that logs on the console.
    BasicConfigurator.configure();

    // grab arguments
    baseDir = args[0];
    outDir = args[1];
    graphFile = args[2];
    typesFile = args[3];
    D = Integer.parseInt(args[4]);

    long time1 = new Date().getTime();

    /**
     * Functionality of SortedEdgeListConstructor
     */
    logger.info("Building Sorted Edge List Index...");

    //load the graph
    graph = new MutableGraph(new File(baseDir, graphFile));

    //load types file
    loadTypesFile();

    // Create Typed Edges
    createTypedEdges();

    // Load and Sort Edges from Graph
    //loadAndSortEdges();
    populateSortedEdgeLists(graph);

    //save the sorted edge lists
    saveSortedEdgeList(MultipleIndexConstructor.outDir);

    //test method that computes totalTypes
    totalTypes = 0;
    computeTotalTypes();
    logger.debug("Computed number of types: " + totalTypes);

    /**
     * Functionality of SPDAndTopologyAndSPathIndexConstructor
     */
    //hash map for all possible "edge-type" paths: i.e. doubles,triples,...D-tuples
    //this gets you the "official" ordering
    logger.info("Computing Edge-Type Path Ordering...");
    computeEdgeTypePathOrdering();


    logger.info("Computing SPD, Topology and SPath Indices...");

    computeIndices(graph);

    long time2 = new Date().getTime();
    logger.info("Time:" + (time2 - time1));

  }

  /**
   * Compute SPD, Topology and SPath Indices
   *
   * @throws IOException
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#computeIndices(String, DBConnection, Graph)
   */
  public static void computeIndicesFast(Graph graph) throws IOException {
    //Make outDir if neccessary
    File directory = new File(outDir);
    if (!directory.exists() && !directory.mkdirs())
      throw new IOException("Could not create directory: " + outDir);
    else {
      logger.info("computeIndicesFast made " + directory.getAbsolutePath());// + " exists " + directory.exists());
    }

    String datasetId = BitcoinBinding.DATASET_ID;// + "_Fast";
    String topologyFilename = datasetId + "." + Integer.toString(D) + ".topology";
    BufferedWriter outTopology = new BufferedWriter(new FileWriter(new File(outDir, topologyFilename)));
    String spdFilename = datasetId + "." + Integer.toString(D) + ".spd";
    File file = new File(outDir, spdFilename);
    BufferedWriter outSPD = new BufferedWriter(new FileWriter(file));
    BufferedWriter counts = new BufferedWriter(new FileWriter(new File(outDir, "counts_" + spdFilename)));

    logger.info("computeIndicesFast Writing to " + file.getAbsolutePath());
    long then = System.currentTimeMillis();
    BitcoinFeaturesBase.logMemory();

    int traversed = 0;
    long totalNeighbors = 0;
    long totalQueued = 0;
    long totalPaths = 0;
    long pathsCopied = 0;

    Map<Integer, NodeInfo> nodeInfos = new HashMap<>();

    makeNodeInfos(graph, nodeInfos);

    if (DEBUG || true) logger.info("computeIndicesFast got " + nodeInfos.size() + " nodes ");
    // D = 2

    Runtime.getRuntime().gc();
    BitcoinFeaturesBase.logMemory();

    long now = doD2(graph, outTopology, outSPD, nodeInfos);

    outTopology.close();
    outSPD.close();
    counts.close();

    // long now = System.currentTimeMillis();
    logger.info("computeIndicesFast took " + ((now - then) / 1000) + " seconds to do graph of size " +
        graph.getNumNodes() + " nodes and " + graph.getNumEdges() + " edges, " +
        " traversed " + traversed +
        " neighbors " + totalNeighbors +
        " queued " + totalQueued + " copied " + pathsCopied + " total paths " + totalPaths
    );

    //if (DEBUG) logger.info("edge to visit " + edgeToVisit);

    BitcoinFeaturesBase.logMemory();
  }

  /**
   * @param graph
   * @param nodeInfos
   * @see #computeIndicesFast(Graph)
   */
  private static void makeNodeInfos(Graph graph, Map<Integer, NodeInfo> nodeInfos) {
    long then = System.currentTimeMillis();
    for (Integer rawID : graph.getRawIDs()) {
      nodeInfos.put(rawID, getNodeInfo(graph, rawID));
    }
    long now = System.currentTimeMillis();
    logger.debug("makeNodeInfos took " + (now - then) + " millis");
  }

  private static void makeNodeInfosParallel(Graph graph, Map<Integer, NodeInfo> nodeInfos) {
    Collection<Integer> rawIDs = graph.getRawIDs();

    List<NodeInfo> nodeInfoList =
        rawIDs.stream()
            .parallel()
            .map(id -> getNodeInfo(graph, id))
            .collect(Collectors.toCollection(ArrayList::new));

    for (NodeInfo nodeInfo : nodeInfoList) {
      nodeInfos.put(nodeInfo.getNodeID(), nodeInfo);
    }
  }

  private static NodeInfo getNodeInfo(Graph graph, Integer rawID) {
    NodeInfo nodeInfo = new NodeInfo(rawID);
    Collection<Edge> values = getUniqueEdges(graph, rawID);

    for (Edge edge : values) {
      int src = edge.getSrc();
      float weight = edge.getFWeight();

//        logger.info("edge " + edge + " src " + src);
      String types = oneHop[getNodeType(src) - 1];
      nodeInfo.addWeight(types, weight, src);

//      if (DEBUG) {
//        logger.info("d1 : for " + count + " : " + edge + " src " +
//            src + " node " + nodeInfo);
//      }
    }
    return nodeInfo;
  }


  private static Collection<Edge> getUniqueEdges(Graph graph, int i) {
    Collection<Edge> nbrs = graph.getNeighbors(i);

    Map<Integer, Edge> srcToEdge = new HashMap<>();
    Map<Integer, Float> srcToWeight = new HashMap<>();

    for (Edge edge : nbrs) {
      int src = edge.getSrc();
      Float edgeWeight = srcToWeight.get(src);
      float fWeight = edge.getFWeight();
      if (edgeWeight == null || edgeWeight < fWeight) {
        srcToWeight.put(src, fWeight);
        srcToEdge.put(src, edge);
      }
    }

    return srcToEdge.values();
  }

  /**
   * @param graph
   * @param outTopology
   * @param outSPD
   * @param nodeInfos
   * @return
   * @throws IOException
   * @see #computeIndicesFast(Graph)
   */
  private static long doD2(Graph graph, BufferedWriter outTopology, BufferedWriter outSPD,
                           Map<Integer, NodeInfo> nodeInfos
  ) throws IOException {
    long then2 = System.currentTimeMillis();

    int count = 0;
    for (Integer rawID : graph.getRawIDs()) {
      logD2(graph, then2, count);
      count++;

      NodeInfo d2 = getD2NodeInfo(graph, nodeInfos, rawID);

      outTopology.write(rawID + "\t");

      NodeInfo nodeInfo = nodeInfos.get(rawID);
      writeTopologyFast(outTopology, 0, nodeInfo.getTypeToInfo());
      writeTopologyFast(outTopology, 1, d2.getTypeToInfo());
      outTopology.write("\n");

      outSPD.write(rawID + "\t");

      writeSPDIndexFast(outSPD, 0, nodeInfo.getTypeToInfo());
      writeSPDIndexFast(outSPD, 1, d2.getTypeToInfo());

      outSPD.write("\n");
    }
    long now = System.currentTimeMillis();
    logger.info("doD2 : took " + ((now - then2)/60000) + " minutes to do D2");
    return now;
  }

  private static NodeInfo getD2NodeInfo(Graph graph, Map<Integer, NodeInfo> nodeInfos, Integer rawID) {
    NodeInfo d2 = new NodeInfo(rawID);

    String theNodeType = oneHop[node2Type.get(rawID) - 1];
    Map<String, Set<Integer>> typeToNeighbors = new HashMap<>();

    for (Edge edge : getUniqueEdges(graph, rawID)) {
      int src = edge.getSrc();
      String neighborType = oneHop[node2Type.get(src) - 1];

      NodeInfo nodeInfo = nodeInfos.get(src);

//        if (DEBUG) logger.info("\tfor " + count + "/" + src + " neighbor " + edge + "/" + neighborType +
//            " and " + nodeInfo);

      float weight = edge.getFWeight();
      for (Map.Entry<String, TypeInfo> pair : nodeInfo.getTypeToInfo().entrySet()) {
        String nType = pair.getKey();
        TypeInfo typeInfo = pair.getValue();
        Set<Integer> neighborNeighbors = typeInfo.getNodes();
//          if (DEBUG) logger.info("\tfor " + count + "/" + src + " type " + nType +
//              " neighborNeighbors " + neighborNeighbors);

        String d2Type = neighborType + nType;
        Set<Integer> seenForType = typeToNeighbors.get(d2Type);

        if (seenForType == null) {
          typeToNeighbors.put(d2Type, seenForType = new HashSet<>());
        }

        for (Integer candidate : neighborNeighbors) {
          if (!candidate.equals(rawID)) {
            seenForType.add(candidate); // TODO : hotspot
          }
        }

        boolean sameType = nType.equals(theNodeType);

        float neighborWeight = typeInfo.getMaxWeight();
        boolean matchingWeight = neighborWeight == weight;
        float weightOfNeighbor = sameType && matchingWeight ? typeInfo.getPrevMax() : neighborWeight;

        int typeCountOfNeighbor = /*(seenForType.contains(i)) ? seenForType.size() - 1 : */seenForType.size();

        if (DEBUG) logger.info("\t\tfor " + src + " type " + nType + " d2 type " + d2Type +
            " and " + typeInfo +
            " match " + matchingWeight + " weight " + weightOfNeighbor + " count " + typeCountOfNeighbor +
            " seen " + seenForType);

        d2.incrMax(d2Type, typeCountOfNeighbor, weightOfNeighbor + weight);

        if (DEBUG) logger.info("\t\td2 " + d2);
      }
    }
    return d2;
  }

  private static void logD2(Graph graph, long then2, int count) {
    if (count % NOTICE_MOD == 0) {
      //System.err.println("Nodes processed: "+i+" out of "+graph.numNodes);
      long now = System.currentTimeMillis();
      long diff = (now - then2) / 1000;
      long l = diff == 0 ? count : count / diff;

      float percent = 100f * (float) count / (float) graph.getNumNodes();
      int ip = Math.round(percent);
      logger.debug("doD2 : Nodes processed: " + count + " out of " + graph.getNumNodes() + " (" + ip + "%) rate " + l + " nodes/sec");
      BitcoinFeaturesBase.logMemory();
    }
    //return count;
  }

  /**
   * @param src internal id?
   * @return
   */
  private static Integer getNodeType(int src) {
    return node2Type.get(src);
  }

  /**
   *
   //setup output files
   //		String filename=graphFile.split("\\.")[0]+"."+Integer.toString(D)+".spath";
   //		logger.info(graphFile);
   //		logger.info(filename);
   //		BufferedWriter out = new BufferedWriter(new FileWriter(new File(outDir, filename)));
   //		String topologyFilename=graphFile.split("\\.")[0]+"."+Integer.toString(D)+".topology";
   //		BufferedWriter outTopology = new BufferedWriter(new FileWriter(new File(outDir, topologyFilename)));
   //		String spdFilename=graphFile.split("\\.")[0]+"."+Integer.toString(D)+".spd";
   //		BufferedWriter outSPD = new BufferedWriter(new FileWriter(new File(outDir, spdFilename)));

   //    String filename = BitcoinBinding.DATASET_ID + "." + Integer.toString(D) + ".spath";
   //    BufferedWriter out = new BufferedWriter(new FileWriter(new File(outDir, filename)));
   */

  /**
   * Compute SPD, Topology and SPath Indices
   *
   * @throws IOException
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#computeIndices(String, DBConnection, Graph)
   */
  public static void computeIndices(Graph graph) throws IOException {

    //Make outDir if neccessary
    File directory = new File(outDir);
    if (!directory.exists() && !directory.mkdirs())
      throw new IOException("Could not create directory: " + outDir);

    String topologyFilename = BitcoinBinding.DATASET_ID + "." + Integer.toString(D) + ".topology";
    BufferedWriter outTopology = new BufferedWriter(new FileWriter(new File(outDir, topologyFilename)));
    String spdFilename = BitcoinBinding.DATASET_ID + "." + Integer.toString(D) + ".spd";
    File file = new File(outDir, spdFilename);
    BufferedWriter outSPD = new BufferedWriter(new FileWriter(file));
    BufferedWriter counts = new BufferedWriter(new FileWriter(new File(outDir, "counts_" + spdFilename)));

    logger.info("Writing to " + file.getAbsolutePath());
    long then = System.currentTimeMillis();
    BitcoinFeaturesBase.logMemory();

    int traversed = 0;
    long totalNeighbors = 0;
    long totalQueued = 0;
    long totalPaths = 0;
    long pathsCopied = 0;
    Map<Edge, Integer> edgeToVisit = new HashMap<>();
    //for(int i=1;i<=graph.numNodes;i++)
    //for (int i = 0; i < graph.getNumNodes(); i++) {
    int count = 0;
    for (Integer rawID : graph.getRawIDs()) {
      if (count % NOTICE_MOD == 0) {
        //System.err.println("Nodes processed: "+i+" out of "+graph.numNodes);
        logger.debug("computeIndices Nodes processed: " + count + " out of " + graph.getNumNodes());
        BitcoinFeaturesBase.logMemory();
      }
      count++;
//      counts.write(i + "," + graph.getRawID(i) + "," + graph.getNeighbors(i).size() +
//          "\n");
      //out.write(i + "\t");
      outTopology.write(rawID + "\t");
      outSPD.write(rawID + "\t");
      //int n=graph.node2NodeIdMap.get(i);//this is a big-bug fixed...
      Set<Integer> queue = new HashSet<>();
      Map<Integer, Double> sumWeight = new HashMap<>();
      queue.add(rawID);
      sumWeight.put(rawID, 0.);
      Set<Integer> considered = new HashSet<>();
      considered.add(rawID);
      Map<Integer, Set<List<Integer>>> paths = new HashMap<>();
      List<Integer> ll = new ArrayList<>();
      ll.add(rawID);
      Set<List<Integer>> hs = new HashSet<>();
      hs.add(ll);
      paths.put(rawID, hs);

      for (int d = 0; d < D; d++) {
        //perform BFS from each node.
        Map<Integer, Set<List<Integer>>> newPaths = new HashMap<>();
        Set<Integer> newQueue = new HashSet<>();
        Map<Integer, Double> newSumWeight = new HashMap<>();

        totalQueued += queue.size();

        if (DEBUG) logger.info("queue " + queue);
        for (int q : queue) {
          Collection<Edge> nbrs = graph.getNeighbors(q);
          totalNeighbors += nbrs.size();

          if (DEBUG) logger.info("nbrs of " + q + " : " + nbrs);

          for (Edge e : nbrs) {
            Integer integer = edgeToVisit.get(e);
            edgeToVisit.put(e, integer == null ? 1 : integer + 1);
            traversed++;
            int qDash = e.getSrc();
            double newWt = sumWeight.get(q) + e.getWeight();

            Double currentWeight = newSumWeight.get(qDash);
            if ((currentWeight != null && currentWeight < newWt) || (currentWeight == null && !considered.contains(qDash))) {
              considered.add(qDash);
              newQueue.add(qDash);
              newSumWeight.put(qDash, newWt);
            } else {
              if (DEBUG)
                logger.info("\tskip " + qDash + " since current weight " + currentWeight + " > " + newWt +
                    " considered " + considered.size() + " already " + considered.contains(qDash));
            }

            boolean hasWeight = newSumWeight.containsKey(qDash);
            boolean visited = considered.contains(qDash);
            if (hasWeight || !visited) {
              if (DEBUG) {
                if (hasWeight != visited) {
                  logger.info("\t1 add path " + qDash + " since hasWeight " + hasWeight + " visited " + visited);
                } else {
                  logger.info("\t2 add path " + qDash + " since hasWeight " + hasWeight + " visited " + visited);

                }
              }

              pathsCopied = getPathsCopied(pathsCopied, newPaths, q, qDash, paths);
              // if (DEBUG) logger.info("pathsCopied  " + pathsCopied);
            } else {
              if (DEBUG) logger.info("\tskip path " + qDash + " since hasWeight " + hasWeight + " visited " + visited);
            }
          }
        }
        queue = newQueue;

        if (DEBUG) logger.info("sumWeight before " + sumWeight);
        sumWeight = newSumWeight;
        if (DEBUG) logger.info("sumWeight after  " + sumWeight);

        if (DEBUG) logger.info("paths before " + paths);
        paths = newPaths;
        if (DEBUG) logger.info("paths after  " + paths);

        totalPaths += paths.size();
        processPathsq(outTopology, outSPD, graph, d, paths);
      }
      //    out.write("\n");
      outTopology.write("\n");
      outSPD.write("\n");
    }
//    out.close();
    outTopology.close();
    outSPD.close();
    counts.close();

    long now = System.currentTimeMillis();
    logger.info("took " + ((now - then) / 1000) + " seconds to do graph of size " +
        graph.getNumNodes() + " nodes and " + graph.getNumEdges() + " edges, " +
        " traversed " + traversed +
        " neighbors " + totalNeighbors +
        " queued " + totalQueued + " copied " + pathsCopied + " total paths " + totalPaths
    );

    if (DEBUG) logger.info("edge to visit " + edgeToVisit);

    BitcoinFeaturesBase.logMemory();
  }

  private static long getPathsCopied(long pathsCopied,
                                     Map<Integer, Set<List<Integer>>> newPaths, int q, int qDash,
                                     Map<Integer, Set<List<Integer>>> paths) {
    Set<List<Integer>> hsai = newPaths.get(qDash);

    if (hsai == null) {
      hsai = new HashSet<>();
      newPaths.put(qDash, hsai);
    }

    for (List<Integer> ai : paths.get(q)) {
      List<Integer> nali = new ArrayList<>(ai);
      pathsCopied++;
      nali.add(qDash);
      hsai.add(nali);
    }
    return pathsCopied;
  }

/*  private static Map<Integer, List<Integer>> getSPathMap(Graph graph, Set<Integer> queue) {
    Map<Integer, List<Integer>> map = new HashMap<>();
    for (int q : queue) {
      int actualID = graph.getRawID(q);
      int label = node2Type.get(actualID);

      List<Integer> al = map.get(label);

      if (al == null) {
        al = new ArrayList<>();
        map.put(label, al);
      }

      al.add(actualID);
    }
    return map;
  }*/

/*
  private static void writeSPath(BufferedWriter out, Map<Integer, List<Integer>> map) throws IOException {
    //for (List<Integer> ids : map.values()) ;

    for (int s = 1; s <= totalTypes; s++) {
      List<Integer> integers = map.get(s);
      if (integers != null) {
        Collections.sort(integers);
        out.write(integers.size() + "#");
        for (int t : integers)
          out.write(t + ",");
      } else {
        out.write("0#,");
      }
      out.write(";");
    }
  }
*/

  private static void processPathsq(Writer outTopology, Writer outSPD, Graph graph, int d,
                                    Map<Integer, Set<List<Integer>>> paths) throws IOException {
    Map<String, Collection<Integer>> topo = new HashMap<>();
    //  Map<Integer, List<Integer>> topo2 = new HashMap<>();

    Map<String, Double> spd = new HashMap<>();
//    Map<Integer, Double> spd2 = new HashMap<>();
    if (DEBUG) logger.info("running with  " + d + " and " + paths.size() + " paths ");

    int[] typeSequence = new int[D];

    for (int ii : paths.keySet()) {
      for (List<Integer> p : paths.get(ii)) {
        String types = "";
//        int typesTotal = 0;
        //logger.info("From " + ii + " p " + p);

        double totWeight = 0;

        for (int j = 0; j < p.size(); j++) {
          int a = p.get(j);

          if (j != 0) {
            int aDash = p.get(j - 1);
            //  Edge edge = graph.getEdge(a, aDash);
            Edge edge = graph.getEdgeFast(a, aDash);

            //if (edge != edgeFast) logger.info("err -- not the same " + edge + " vs " +edgeFast);

            totWeight += edge.getWeight();
            Integer typeOfDestNode = getNodeType(a);
            // types += typeOfDestNode; // TODO : string concatenation...

            typeSequence[j - 1] = typeOfDestNode - 1;
//            typesTotal++;
          }

          if (j == 1) {
            types = oneHop[typeSequence[0]];
          } else if (j == 2) {
            types = twoHop[typeSequence[0]][typeSequence[1]];
          }
        }

        //   logger.info("From " + ii + " p " + p + " types " + types);

        Double currentWeight = spd.get(types);
        if ((currentWeight != null && currentWeight < totWeight) || currentWeight == null) {
          spd.put(types, totWeight);
        }

        updateTopo(topo, ii, types);
      }
    }

    //write out topology index.
    writeTopology(outTopology, d, topo);
    //outTopology.write(" ");

    //write out spd index.
    writeSPDIndex(outSPD, d, spd);
    //outSPD.write(" ");
  }

  private static void writeTopology(Writer outTopology, int d, Map<String, Collection<Integer>> topo) throws IOException {
    for (String o : ordering.get(d + 1)) {
      String toWrite;

      if (!topo.containsKey(o)) {
        toWrite = "0;";
      } else {
        int t = topo.get(o).size();
        toWrite = t + ";";
      }
      if (DEBUG) logger.info("writeTopology for  " + d + " : " + toWrite);

      outTopology.write(toWrite);
    }
  }


  private static void writeTopologyFast(Writer outTopology, int d, Map<String, TypeInfo> topo) throws IOException {
    for (String o : ordering.get(d + 1)) {
      String toWrite;

      if (!topo.containsKey(o)) {
        toWrite = "0;";
      } else {
        int t = topo.get(o).getCount();
        toWrite = t + ";";
      }
      //  if (DEBUG) logger.info("writeTopologyFast for  " + d + " : " + toWrite);

      outTopology.write(toWrite);
    }
  }


  private static void writeSPDIndex(Writer outSPD, int d, Map<String, Double> spd) throws IOException {
    for (String o : ordering.get(d + 1)) {
      String str = "0.;";
      if (!spd.containsKey(o)) {
        str = "0.;";
      } else {
        double t = spd.get(o);
        String format = twoDForm.format(t);
        //   str = Double.valueOf(format) + ";";
        str = format + ";";
      }
      //if (DEBUG) logger.info("writeSPDIndex for  " + d + " : " + str);

      outSPD.write(str);
    }
  }

  private static void writeSPDIndexFast(Writer outSPD, int d, Map<String, TypeInfo> spd) throws IOException {
    for (String o : ordering.get(d + 1)) {
      String str = "0.";
      if (spd.containsKey(o)) {
        float t = spd.get(o).getMaxWeight();
        //   str = Double.valueOf(format) + ";";
        str = twoDForm.format(t);
      }
      //     if (DEBUG) logger.info("writeSPDIndexFast for  " + d + " : " + str);

      outSPD.write(str);
      outSPD.write(";");
    }
  }


  private static void updateTopo(Map<String, Collection<Integer>> topo, int ii, String types) {
//    int lastNode = ii;
    Collection<Integer> l = topo.get(types);

    if (l == null) {
      l = new HashSet<>();
      topo.put(types, l);
    }

    if (!l.contains(ii)) {
      l.add(ii);
    }
  }

  /**
   * Compute all EdgeType Paths of length d, put in hash map
   *
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#computeIndices(String, DBConnection, Graph)
   */
  public static void computeEdgeTypePathOrdering() {
    for (int d = 1; d <= D; d++)
      ordering.put(d, new ArrayList<>());
    for (int i = 1; i <= totalTypes; i++)
      ordering.get(1).add(i + "");

    for (int d = 2; d <= D; d++) {
      for (int i = 1; i <= totalTypes; i++) {
        for (String s : ordering.get(d - 1)) {
          if (s.length() == d - 1)
            ordering.get(d).add(s + i);
        }
      }
    }

    logger.info("computeEdgeTypePathOrdering ordering " + ordering);

  }

  /**
   * Figure out how many types there are (in the case where we're not loading everything from file)
   */
  public static void computeTotalTypes() {
    for (int key : node2Type.keySet()) {
      int type = node2Type.get(key);
      if (type > totalTypes)
        totalTypes = type;
    }
  }

  /**
   * Save out sorted edge list
   *
   * @throws IOException
   * @paramx graphFile
   * @seex TopKTest#getGraphBeforeComputeIndices
   */
  public static void saveSortedEdgeList(String outDir) throws IOException {
    logger.info("out dir " + outDir);
    //Make outDir if neccessary
    File directory = new File(outDir);
    if (!directory.exists() && !directory.mkdirs())
      throw new IOException("Could not create directory: " + outDir);

    //Save out....
    for (String key : sortedEdgeLists.keySet()) {
      //String fileName=graphFile.split("\\.")[0]+"_"+key+".list";
      String fileName = BitcoinBinding.DATASET_ID + "_" + key + ".list";

      File file = new File(outDir, fileName);
      logger.info("writing sorted edge lists to " + file.getAbsolutePath());
      BufferedWriter out = new BufferedWriter(new FileWriter(file));
      List<Edge> arr = sortedEdgeLists.get(key);
      for (Edge e : arr)
        out.write(e.getSrc() + "#" + e.getDst() + "#" + e.getWeight() + "\n");
      out.close();
    }
  }

  /**
   * Load and sort edges from Graph transactions file
   *
   * @throws FileNotFoundException
   * @throws IOException
   * @paramx graphFile
   */
/*
  public static void loadAndSortEdges()
      throws IOException {

    //load edges
    BufferedReader in = new BufferedReader(new FileReader(new File(baseDir, graphFile)));
    String str = "";
    while ((str = in.readLine()) != null) {
      if (str.startsWith("#"))
        continue;
      String tokens[] = str.split("#");
      int from = Integer.parseInt(tokens[0]);
      int to = Integer.parseInt(tokens[1]);
      if (from > to)
        continue;
      double wt = Double.parseDouble(tokens[2]);
      int fromType = node2Type.get(from);
      int toType = node2Type.get(to);
      if (fromType > toType) {
        int tmp = fromType;
        fromType = toType;
        toType = tmp;
        tmp = from;
        from = to;
        to = tmp;
      }
      Edge e = new Edge(from, to, wt);
      ArrayList<Edge> arr = sortedEdgeLists.get(fromType + "#" + toType);
      arr.add(e);
      sortedEdgeLists.put(fromType + "#" + toType, arr);
    }
    in.close();

    //sort the arraylists in descending order
    for (String key : sortedEdgeLists.keySet())
      Collections.sort(sortedEdgeLists.get(key), new EdgeComparator());
  }
*/

  /**
   * Sister method to loadAndSortEdges.
   * --Does the same thing, just from the graph file loaded in
   * --as a IndexConstruction.Graph data structure
   *
   * @paramx None (except it expects class variables graph and node2Type have been populated)
   */
  public static void populateSortedEdgeLists(Graph graph) {
    if (node2Type.isEmpty()) {
      logger.warn("node2Type is empty?");
    } else {
      logger.info("populateSortedEdgeLists node2Type size " + node2Type.size());
    }

    int skipped = 0;

    for (Edge e : graph.getEdges()) {
      //get internal node-ids for
      //    logger.info("populateSortedEdgeLists edge " + e);
//      int from = graph.getRawID(e.getSrc());
//      int to = graph.getRawID(e.getDst());
      int from = e.getSrc();
      int to = e.getDst();

      double weight = e.getWeight();

//      logger.info("from index is: " + from + " to index is: " + to);

      if (from > to)
        continue;


      Integer fromInt = node2Type.get(from);
      Integer toInt = node2Type.get(to);
      if (fromInt != null && toInt != null) {
        int fromType = fromInt;
        int toType = toInt;
        if (fromType > toType) {
          int tmp = fromType;
          fromType = toType;
          toType = tmp;
          tmp = from;
          from = to;
          to = tmp;
        }
        List<Edge> arr = sortedEdgeLists.get(fromType + "#" + toType);
        arr.add(new Edge(from, to, weight));
        sortedEdgeLists.put(fromType + "#" + toType, arr);
      } else {
        skipped++;
        if (skipped < 20) {
          logger.warn("skipping missing user " +
              (fromInt == null ? " from " + from : "") +
              (toInt == null ? " to " + to : "")
          );
        }
      }
    }

    logger.warn("populateSortedEdgeLists skipped " + skipped + " out of " + graph.getNumNodes());
    //sort the arraylists in descending order
    for (String key : sortedEdgeLists.keySet())
      Collections.sort(sortedEdgeLists.get(key), new EdgeComparator());
  }

  /**
   * Create "typed edges" i.e. node of type i is connected to node of type j
   *
   * @paramx totalTypes
   */
  public static void createTypedEdges() {

    //create typed edges
    for (int t = 1; t <= totalTypes; t++) {
      for (int t2 = t; t2 <= totalTypes; t2++) {
        String key = t + "#" + t2;
        sortedEdgeLists.put(key, new ArrayList<>());
      }
    }
  }

  /**
   * Load types file
   *
   * @throws FileNotFoundException
   * @throws IOException
   * @see #main(String[])
   */
  private static void loadTypesFile()
      throws IOException {

    //load types file
    BufferedReader in = new BufferedReader(new FileReader(new File(baseDir, typesFile)));
    String str = "";
    while ((str = in.readLine()) != null) {
      String tokens[] = str.split("\\t");
      int node = Integer.parseInt(tokens[0]);
      int type = Integer.parseInt(tokens[1]);
      node2Type.put(node, type);
      if (type > totalTypes)
        totalTypes = type;
    }
    in.close();
  }

  /**
   * @paramx n
   * @seex TopKTest#ingest
   * @seex TopK#
   */
/*  public static Collection<Integer> loadTypes(int n) {
    for (int i = 0; i < n; i++) node2Type.put(i, 1);
    return setTotalTypes();
  }*/

  /**
   * @param node2TypeToUse
   * @return
   * @seex TopKTest#beforeComputeIndicesMod
   */
  public static Collection<Integer> loadTypes(Map<Integer, Integer> node2TypeToUse) {
    node2Type = node2TypeToUse;
    return setTotalTypes();
  }

  private static Set<Integer> setTotalTypes() {
    Set<Integer> integers = new HashSet<>(node2Type.values());
    totalTypes = integers.size();
    return integers;
  }

  /**
   * Load types info from database
   *
   * @param dbConnection
   * @return
   * @throws Exception
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#computeIndices(String, DBConnection)
   */
  public static void loadTypesFromDatabase(DBConnection dbConnection, String tableName, String uidColumn, String typeColumn)
      throws Exception {

//    logger.info("loadTypesFromDatabase " + tableName + " " + uidColumn + " " + typeColumn);

		/*
     * Do query
		 */
    Connection connection = dbConnection.getConnection();

    String sqlQuery = "select " + uidColumn + ", " + typeColumn + " from " + tableName + ";";

    //  logger.info("loadTypesFromDatabase sql " + sqlQuery + " on " + connection);

    PreparedStatement queryStatement = connection.prepareStatement(sqlQuery);
    ResultSet rs = queryStatement.executeQuery();

    long then = System.currentTimeMillis();

    Set<Integer> types = new HashSet<>();
    /*
     * Loop-through result set, populate node2Type
		 */
    int c = 0;
    while (rs.next()) {
      c++;
      if (c % 100000 == 0) {
        logger.debug("read  " + c);
      }

      //Retrieve by column name
      int guid = rs.getInt(uidColumn);
      int type = rs.getInt(typeColumn);

      //logger.info("UID: "+guid+"\tTYPE: "+type);
      node2Type.put(guid, type);
      types.add(type);
      if (type > totalTypes)
        totalTypes = type;
    }
    long now = System.currentTimeMillis();

    logger.info("loadTypesFromDatabase : took " + (now - then) +
        " to populate node2Type, size " + node2Type.size() + " with " + c + " total types " + totalTypes + " count types " + types.size() + " types " + types);

    rs.close();
    queryStatement.close();

    makeTypeIDs(types);
  }

  /**
   * @param types
   * @see #loadTypesFromDatabase(DBConnection, String, String, String)
   */
  public static void makeTypeIDs(Collection<Integer> types) {
//    logger.info("makeTypeIDs " + types);
    oneHop = new String[types.size()];
    twoHop = new String[types.size()][types.size()];

    int i = 0;
    for (Integer t : types) {
      String firstType = Integer.toString(t);
      oneHop[i] = firstType;
      int j = 0;
      for (Integer tt : types) {
        String secondType = Integer.toString(tt);
        //    twoHop[i][j++] = firstType+"_"+secondType;
        String both = firstType + secondType;
        twoHop[i][j++] = both;
        //logger.info("makeTypeIDs added " + both);
      }
      i++;
    }

    logger.info("makeTypeIDs one " + Arrays.asList(oneHop));
    logger.info("makeTypeIDs two hop " + Arrays.asList(twoHop));
  }

  private static String[] oneHop;
  private static String[][] twoHop;

  /**
   * Setter for pre-loaded node2Type HashMap
   *
   * @see IngestAndQuery#computeIndices()
   */
  public static void setNode2Type(HashMap<Integer, Integer> in) {
    node2Type = in;
  }

  /**
   * Setter for pre-loaded graph graph
   */
  public static void setGraph(Graph in) {
    graph = in;
  }

  /**
   * Getter for graph graph
   */
  public static Graph getGraph() {
    return graph;
  }
}
