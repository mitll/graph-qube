/*
 * Copyright 2013-2016 MIT Lincoln Laboratory, Massachusetts Institute of Technology
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mitll.xdata.binding;

import influent.idl.FL_Entity;
import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_PatternSearchResult;
import mitll.xdata.GraphQuBEServer;
import mitll.xdata.ServerProperties;
import mitll.xdata.SimplePatternSearch;
import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesBase;
import mitll.xdata.dataset.bitcoin.features.MyEdge;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.db.MysqlConnection;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import uiuc.topksubgraph.Graph;
import uiuc.topksubgraph.MultipleIndexConstructor;
import uiuc.topksubgraph.MutableGraph;
import uiuc.topksubgraph.QueryExecutor;

import java.io.IOException;
import java.util.*;

/**
 * Created by go22670 on 1/8/16.
 */
@RunWith(JUnit4.class)
public class TopKTest {
  private static final String DATA_FAST = "data/bitcoin/fast/";
  private static Logger logger = Logger.getLogger(TopKTest.class);

  @Test
  public void testSimpleSearchFast() {
    int n = 10;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 5);
    String outdir = DATA_FAST;

    Graph graph = ingestFast(edgeToWeight, outdir);

    Map<Long, Integer> idToType = getUniformTypes(graph);

    logger.info("id to type " + idToType);

    QueryExecutor executor = new QueryExecutor(graph,
        "bitcoin_small",
        getResourcePath(outdir), idToType);

    doTests(graph, idToType, executor);
  }

  @Test
  public void testSimpleSearchFast2() {
    int n = 10;
    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 5);
    String outdir = DATA_FAST;

    Graph graph = ingestFastMod(edgeToWeight, outdir, 2);

    Map<Long, Integer> idToType = getModTypes(graph, 2);

    QueryExecutor executor = new QueryExecutor(graph,
        "bitcoin_small",
        getResourcePath(outdir), idToType);

    doTests(graph, idToType, executor);
  }

  @Test
  public void testSimpleSearchFast4() {
    int n = 20;
    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 3);
    String outdir = DATA_FAST;

    int mod = 4;
    Graph graph = ingestFastMod(edgeToWeight, outdir, mod);

    Map<Long, Integer> idToType = getModTypes(graph, mod);

    QueryExecutor executor = new QueryExecutor(graph,
        "bitcoin_small",
        getResourcePath(outdir), idToType);

    doTests(graph, idToType, executor);
  }

  @Test
  public void testSimpleSearch() {
    int n = 10;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 2);
    String outdir = "data/bitcoin/indices/";
    Graph graph = ingest(n, edgeToWeight, outdir);

    Map<Long, Integer> idToType = getUniformTypes(graph);

    QueryExecutor executor = new QueryExecutor(graph,
        "bitcoin_small",
        getResourcePath(outdir), idToType);


    doTests(graph, idToType, executor);
  }

  /**
   * List<String> exemplarIDs = Arrays.asList("10", "20", "30");
   * List<String> exemplarIDs2 = Arrays.asList("20", "40", "70");
   * List<String> exemplarIDs3 = Arrays.asList("10", "20");
   * List<String> exemplarIDs4 = Arrays.asList("20", "70", "80");
   *
   * @param graph
   * @param idToType
   * @param executor
   */

  private void doTests(Graph graph, Map<Long, Integer> idToType, QueryExecutor executor) {
    //  List<String> exemplarIDs = Arrays.asList("1", "2", "3");

    List<String> exemplarIDs = Arrays.asList("10", "20", "40");
    List<String> exemplarIDs2 = Arrays.asList("20", "40", "70");
    List<String> exemplarIDs3 = Arrays.asList("10", "20");
    List<String> exemplarIDs4 = Arrays.asList("20", "70", "80");


    /*    List<String> exemplarIDs2 = Arrays.asList("2", "4", "7");
    List<String> exemplarIDs3 = Arrays.asList("1", "2");
    List<String> exemplarIDs4 = Arrays.asList("2", "7", "8");*/

    testQuery(graph, idToType, executor, exemplarIDs);
    testQuery(graph, idToType, executor, exemplarIDs2);
    testQuery(graph, idToType, executor, exemplarIDs3);
    testQuery(graph, idToType, executor, exemplarIDs4);

  }

  private void testQuery(Graph graph, Map<Long, Integer> idToType, QueryExecutor executor, List<String> exemplarIDs) {
    try {
      boolean valid = executor.testQuery(exemplarIDs, graph, idToType);
      if (!valid) logger.info(exemplarIDs + " are not connected");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * @param graph
   * @return
   * @see #testSimpleSearchFast()
   */
  private Map<Long, Integer> getUniformTypes(Graph graph) {
    Map<Long, Integer> idToType = new HashMap<>();
    for (Long rawID : graph.getRawIDs()) {
      int type = 1;
      idToType.put(rawID, type);
//      idToType.put(graph.getInternalID(rawID), type);
    }
    return idToType;
  }

  @Test
  public void testSimpleSearchFastOddEven2() {
    int n = 10;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 5);

    String outdir = DATA_FAST;

    Graph graph = ingestFastMod(edgeToWeight, outdir, 2);

    Map<Long, Integer> idToType = getModTypes(graph, 2);

    QueryExecutor executor = new QueryExecutor(graph,
        "bitcoin_small",
        getResourcePath(outdir), idToType);


    doOddEvenTests(graph, idToType, executor);
  }

  private String getResourcePath(String outdir) {
    String base = "/Users/go22670/Projects/graph-qube/";
    String baseToUse = "/Users/go22670/graph-qube/";
    return baseToUse + outdir;
  }

  @Test
  public void testSimpleSearch2() {
    int n = 10;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 5);

    String outdir = "data/bitcoin/indices/";

    Graph graph = ingestMod(n, edgeToWeight, outdir, 2);

    Map<Long, Integer> idToType = getModTypes(graph, 2);

    QueryExecutor executor = new QueryExecutor(graph,
        "bitcoin_small",
        getResourcePath(outdir), idToType);


    doOddEvenTests(graph, idToType, executor);
  }

  private void doOddEvenTests(Graph graph, Map<Long, Integer> idToType, QueryExecutor executor) {
    List<String> exemplarIDs = Arrays.asList("30", "50", "70");
    List<String> exemplarIDs2 = Arrays.asList("2", "4", "6");
    List<String> exemplarIDs3 = Arrays.asList("2", "4", "6", "8");
    List<String> exemplarIDs5 = Arrays.asList("1", "4", "5", "8");

    List<String> exemplarIDs4 = Arrays.asList("1", "7", "9");


    executor.testQuery(exemplarIDs, graph, idToType);
    executor.testQuery(exemplarIDs2, graph, idToType);
    executor.testQuery(exemplarIDs3, graph, idToType);
    executor.testQuery(exemplarIDs4, graph, idToType);
    executor.testQuery(exemplarIDs5, graph, idToType);
  }

  /**
   * TODO : evil - we store both internal and raw node ids -> type
   *
   * @param graph
   * @param mod
   * @return
   */
  private Map<Long, Integer> getModTypes(Graph graph, int mod) {
    Map<Long, Integer> idToType = new HashMap<>();
    for (Long rawID : graph.getRawIDs()) {
      int type = (int)(rawID % mod) + 1;
      idToType.put(rawID, type);
//      idToType.put(graph.getInternalID(rawID), type);
    }
    return idToType;
  }

  @Test
  public void testSearch() {
    logger.debug("ENTER testSearch()");
    ServerProperties props = new ServerProperties("vermont.properties");

    String bitcoinDirectory = ".";
   // String bitcoinFeatureDirectory = GraphQuBEServer.DEFAULT_BITCOIN_FEATURE_DIR;

    try {
      DBConnection dbConnection = props.useMysql() ? new MysqlConnection(props.getSourceDatabase()) :
          new H2Connection(bitcoinDirectory, props.getFeatureDatabase());
      final SimplePatternSearch patternSearch;
      patternSearch = new SimplePatternSearch();

      BitcoinBinding bitcoinBinding = new BitcoinBinding(dbConnection, props);
      patternSearch.setBitcoinBinding(bitcoinBinding);
      Shortlist shortlist = bitcoinBinding.getShortlist();
      int max = 20;

      long then = System.currentTimeMillis();
      // 397635298	533869146
      // 163694973
     // List<String> exemplarIDs = Arrays.asList("248750138","397635298", "533869146");
      List<String> exemplarIDs = Arrays.asList("163694973","248750138","397635298", "533869146");
      List<FL_PatternSearchResult> shortlist1 = shortlist.getShortlist(null, exemplarIDs, max);
      long now = System.currentTimeMillis();
      logger.info("to get " + shortlist1.size() +
           " took to do a search " + (now - then) + " millis ");

      if (shortlist1.size() > max) {
        shortlist1 = shortlist1.subList(0, max);
      }

      long hashtotal = 0;
      for (FL_PatternSearchResult result : shortlist1) {
        // logger.info("got " + result);
        Collection<String> matches = new ArrayList<>();
        for (FL_EntityMatchResult entity : result.getEntities()) {
          FL_Entity entity1 = entity.getEntity();
          matches.add(entity1.getUid());
          hashtotal += entity1.getUid().hashCode();
          //logger.info("got " + entity1.getUid()+ " " + entity.getScore());
        }
        logger.info("got  match " + matches);
      }
      logger.info("got hash code total " + hashtotal + " match " + shortlist1.size());

    } catch (Exception e) {
      logger.error("got " + e, e);

    }

//    Assert.assertEquals(sequence.getStates(), makeStates(1, 1, 1, 2, 2, 2));

    logger.debug("EXIT testSearch()");
  }

  @Test
  public void testGraph1() {
    logger.debug("ENTER testGraph1()");
    ServerProperties props = new ServerProperties("vermont.properties");
    int n = 100000;
    int neighbors = 10;
    BitcoinFeaturesBase.rlogMemory();

    try {
      int max = 64;//128;
      for (int i = 32; i < max; i *= 2) {
        long time1 = System.currentTimeMillis();
        BitcoinFeaturesBase.rlogMemory();
        logger.info(n + " and " + i + " -------------------- ");

        Map<MyEdge, Integer> edgeToWeight = getGraph(n, i);

        Graph graph = new MutableGraph(edgeToWeight, false);

        Runtime.getRuntime().gc();

        BitcoinFeaturesBase.rlogMemory();

        long time2 = new Date().getTime();
        logger.info("Time:" + (time2 - time1));
      }
    } catch (Exception e) {
      logger.error("got " + e, e);
    }

    logger.debug("EXIT testSearch()");
    sleep();
  }

  private void sleep() {
    try {
      Thread.sleep(1000000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testIngest() {
    logger.debug("ENTER testIngest()");
    int n = 5;
    //int neighbors = 100;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 1);
    String outdir = "data/bitcoin/indices/";

    ingest(n, edgeToWeight, outdir);

    //sleep();

    logger.debug("EXIT testIngest()");
  }

  @Test
  public void testIngest2() {
    logger.debug("ENTER testIngest()");
    int n = 10;
    //int neighbors = 100;
    String outdir = "data/bitcoin/indices/";

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 2);
    ingestMod(n, edgeToWeight, outdir, 2);

    String outdirFast = DATA_FAST;

    ingestFastMod(edgeToWeight, outdirFast, 2);

    //sleep();

    logger.debug("EXIT testIngest()");
  }

  @Test
  public void testIngestFast() {
    logger.debug("ENTER testIngestFast()");
    int n = 5;
    //int neighbors = 100;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 1);
    String outdir = DATA_FAST;
    ingestFast(edgeToWeight, outdir);

    //sleep();

    logger.debug("EXIT testIngestFast()");
  }

  @Test
  public void testIngestTwo() {
    logger.debug("ENTER testIngestTwo()");
    int n = 5;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 2);
    String outdir = "data/bitcoin/indices/";

    ingest(n, edgeToWeight, outdir);
    ingestFast(edgeToWeight, outdir.replaceAll("indices", "fast"));

    logger.debug("EXIT testIngestTwo()");
  }

  @Test
  public void testIngestTwoSmallest() {
    logger.debug("ENTER testIngestTwo()");
    int n = 5;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 1);
    String outdir = "data/bitcoin/indices/";

    ingest(n, edgeToWeight, outdir);
    ingestFast(edgeToWeight, outdir.replaceAll("indices", "fast"));

    logger.debug("EXIT testIngestTwo()");
  }

  @Test
  public void testIngestTwoTypes() {
    logger.debug("ENTER testIngestTwo()");
    int n = 5;
    //int neighbors = 100;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 2);
    String outdir = "data/bitcoin/indices/";
    ingest(n, edgeToWeight, outdir);

    String outFast = outdir.replaceAll("indices", "fast");
    ingestFast(edgeToWeight, outFast);

    //sleep();

    logger.debug("EXIT testIngestTwo()");
  }

  @Test
  public void testIngestLarger() {
    logger.debug("ENTER testIngestTwo()");
    int n = 400000;
    //int neighbors = 100;

    Map<MyEdge, Integer> edgeToWeight = getGraph(n, 8);

    long then = System.currentTimeMillis();

    String outdir = "data/bitcoin/indices/";

    ingest(n, edgeToWeight, outdir);

    long then2 = System.currentTimeMillis();

    logger.info("old " + (then2 - then));

    String outFast = outdir.replaceAll("indices", "fast");

    ingestFast(edgeToWeight, outFast);
    long now = System.currentTimeMillis();
    logger.info("new " + (now - then2));

    //sleep();

    logger.debug("EXIT testIngestTwo()");
  }


  @Test
  public void testGraph() {
    logger.debug("ENTER testSearch()");
    int n = 400000;
    //int neighbors = 100;

    String outdir = "data/bitcoin/indices/";
    for (int i = 10; i < 50; i += 10) {
      Map<MyEdge, Integer> edgeToWeight = getGraph(n, i);
      ingest(n, edgeToWeight, outdir);
    }

    sleep();

    logger.debug("EXIT testSearch()");
  }

  boolean populateInLinks2 = false; //for now

  private Graph ingestMod(int n, Map<MyEdge, Integer> edgeToWeight, String outdir, int mod) {
    try {
      long time1 = System.currentTimeMillis();

      Graph graph = beforeComputeIndicesMod(edgeToWeight, outdir, mod, populateInLinks2);

      computeIndices(time1, graph);

      return graph;
    } catch (Exception e) {
      logger.error("got " + e, e);
    }
    return null;
  }

  private Graph ingest(int n, Map<MyEdge, Integer> edgeToWeight, String outdir) {
    try {
      long time1 = System.currentTimeMillis();

      Graph graph = beforeComputeIndices(edgeToWeight, outdir, populateInLinks2);

      computeIndices(time1, graph);

      return graph;
    } catch (Exception e) {
      logger.error("got " + e, e);
    }
    return null;
  }

  private void computeIndices(long time1, Graph graph) throws IOException {
    long then = System.currentTimeMillis();
    BitcoinFeaturesBase.rlogMemory();
    MultipleIndexConstructor.computeIndices(graph, "test");
    BitcoinFeaturesBase.rlogMemory();

    long time2 = new Date().getTime();
    logger.info("Time:" + (time2 - time1));
    logger.info("Time to do computeIndices :" + (time2 - then));
  }

  private Graph ingestFast(Map<MyEdge, Integer> edgeToWeight, String outdir) {
    try {
      long time1 = System.currentTimeMillis();

      Graph graph = beforeComputeIndices(edgeToWeight, outdir, populateInLinks2);
      computeIndicesFast(time1, graph);

      return graph;
    } catch (Exception e) {
      logger.error("got " + e, e);
    }
    return null;
  }

  /**
   * @param edgeToWeight
   * @param outdir
   * @param mod
   * @return
   * @see #testIngest2()
   */
  private Graph ingestFastMod(Map<MyEdge, Integer> edgeToWeight, String outdir, int mod) {
    try {
      long time1 = System.currentTimeMillis();

      Graph graph = beforeComputeIndicesMod(edgeToWeight, outdir, mod, populateInLinks2);
      computeIndicesFast(time1, graph);

      return graph;
    } catch (Exception e) {
      logger.error("got " + e, e);
    }
    return null;
  }

  private void computeIndicesFast(long time1, Graph graph) throws IOException {
    long then = System.currentTimeMillis();
    BitcoinFeaturesBase.rlogMemory();
    MultipleIndexConstructor.outDir = MultipleIndexConstructor.outDir.replaceAll("indices", "fast");
    MultipleIndexConstructor.computeIndicesFast(graph, "test");
    BitcoinFeaturesBase.rlogMemory();

    long time2 = new Date().getTime();
    logger.info("Time:" + (time2 - time1));
    logger.info("Time to do computeIndices :" + (time2 - then));
  }

  /**
   * @param edgeToWeight
   * @param outDir
   * @param mod
   * @return
   * @throws IOException
   * @see #ingestFastMod(Map, String, int)
   * @see #ingestMod(int, Map, String, int)
   */
  private Graph beforeComputeIndicesMod(Map<MyEdge, Integer> edgeToWeight, String outDir, int mod, boolean populateInLinks2) throws IOException {
    Map<Long, Integer> node2Type = loadTypesMod(edgeToWeight.keySet(), mod);
    Collection<Integer> types = MultipleIndexConstructor.loadTypes(node2Type);
    return getGraphBeforeComputeIndices(edgeToWeight, types, outDir, populateInLinks2);
  }

  private Graph beforeComputeIndices(Map<MyEdge, Integer> edgeToWeight, String outDir, boolean populateInLinks2) throws IOException {
    return beforeComputeIndicesMod(edgeToWeight, outDir, 1, populateInLinks2);
  }

  private Map<Long, Integer> loadTypesMod(Collection<MyEdge> ids, int mod) {
    Map<Long, Integer> node2Type = new HashMap<>();
    for (MyEdge id : ids) {
      long low = BitcoinFeaturesBase.getLow(id);
      long high = BitcoinFeaturesBase.getHigh(id);

      node2Type.put(low, (int) ((low % mod) + 1));
      node2Type.put(high, (int) ((high % mod) + 1));
    }
    return node2Type;
  }

  private Graph getGraphBeforeComputeIndices(Map<MyEdge, Integer> edgeToWeight,
                                             Collection<Integer> types,
                                             String outDir, boolean populateInLinks2) throws IOException {
    BitcoinFeaturesBase.logMemory();

    Graph graph = new MutableGraph(edgeToWeight, populateInLinks2);

    BitcoinFeaturesBase.logMemory();

    MultipleIndexConstructor.createTypedEdges();

    // Create Typed Edges

    // Load and Sort Edges from Graph
    MultipleIndexConstructor.populateSortedEdgeLists(graph);
    BitcoinFeaturesBase.logMemory();

    //save the sorted edge lists
    //String outDir = MultipleIndexConstructor.outDir;
    MultipleIndexConstructor.saveSortedEdgeList(outDir, "test");
    BitcoinFeaturesBase.logMemory();

    //test method that computes totalTypes
    MultipleIndexConstructor.computeTotalTypes();
    BitcoinFeaturesBase.logMemory();
    // logger.debug("Computed number of types: " + totalTypes);

    /**
     * Functionality of SPDAndTopologyAndSPathIndexConstructor
     */
    //hash map for all possible "edge-type" paths: i.e. doubles,triples,...D-tuples
    //this gets you the "official" ordering
    logger.info("Computing Edge-Type Path Ordering...");
    MultipleIndexConstructor.computeEdgeTypePathOrdering();


    logger.info("Computing SPD, Topology and SPath Indices...");

//    Set<Integer> types = new HashSet<>(2);
//    types.add(1);
    MultipleIndexConstructor.makeTypeIDs(types);
    return graph;
  }

  private Map<MyEdge, Integer> getGraph(int n, int neighbors) {
    Map<MyEdge, Integer> edgeToWeight = new HashMap<>();

    Random random = new Random(123456789l);

    for (int from = 0; from < n; from++) {
      Set<Long> current = new HashSet<>();

      for (int j = 0; j < neighbors; j++) {
        long to = random.nextInt(n);
        while (to == from || current.contains(to)) {
          to = random.nextInt(n);
        }

        long rawFrom = getRawID(from);
        long rawTo = getRawID(to);

        MyEdge l = BitcoinFeaturesBase.storeTwo(rawFrom, rawTo);
        current.add(to);
    /*    int low = BitcoinFeaturesBase.getLow(l);
        int high = BitcoinFeaturesBase.getHigh(l);
        if (low != from) logger.error("huh?");
        if (high != to) logger.error("huh?");
    */
        int w = 1 + random.nextInt(9);
//        logger.info(rawFrom + "->" + rawTo + " : " + w);
        edgeToWeight.put(l, w);
      }
    }

    logger.info("made " + edgeToWeight.size() + " with " + edgeToWeight.keySet().size() + " and " + edgeToWeight.values().size());
    return edgeToWeight;
  }

  private long getRawID(long from) {
    return 10 * from;
  }
}