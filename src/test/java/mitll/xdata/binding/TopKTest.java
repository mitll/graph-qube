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

import influent.idl.FL_EntityMatchResult;
import influent.idl.FL_PatternSearchResult;
import mitll.xdata.GraphQuBEServer;
import mitll.xdata.ServerProperties;
import mitll.xdata.SimplePatternSearch;
import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesBase;
import mitll.xdata.db.DBConnection;
import mitll.xdata.db.H2Connection;
import mitll.xdata.db.MysqlConnection;
import org.apache.batik.css.engine.SystemColorSupport;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SyslogAppender;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import uiuc.topksubgraph.Graph;
import uiuc.topksubgraph.MultipleIndexConstructor;

import java.io.IOException;
import java.util.*;

/**
 * Created by go22670 on 1/8/16.
 */
@RunWith(JUnit4.class)
public class TopKTest {
  private static Logger logger = Logger.getLogger(TopKTest.class);


  @Test
  public void testSearch() {
    logger.debug("ENTER testSearch()");
    ServerProperties props = new ServerProperties();

    String bitcoinDirectory = ".";
    String bitcoinFeatureDirectory = GraphQuBEServer.DEFAULT_BITCOIN_FEATURE_DIR;

    try {
      DBConnection dbConnection = props.useMysql() ? new MysqlConnection(props.mysqlBitcoinJDBC()) : new H2Connection(bitcoinDirectory,"bitcoin");
      final SimplePatternSearch patternSearch;
      patternSearch = new SimplePatternSearch();

      BitcoinBinding bitcoinBinding = new BitcoinBinding(dbConnection, bitcoinFeatureDirectory);
      patternSearch.setBitcoinBinding(bitcoinBinding);
      Shortlist shortlist = bitcoinBinding.getShortlist();
      List<FL_PatternSearchResult> shortlist1 = shortlist.getShortlist(null, Arrays.asList("555261", "400046", "689982", "251593"), 10);

      if (shortlist1.size() > 10) {
        shortlist1 = shortlist1.subList(0,10);
      }

      for (FL_PatternSearchResult result:shortlist1) {
        logger.info("got " + result);
        for (FL_EntityMatchResult entity : result.getEntities()) {
          logger.info("got " + entity);
        }
      }

    } catch (Exception e) {
      logger.error("got " + e,e);

    }

//    Assert.assertEquals(sequence.getStates(), makeStates(1, 1, 1, 2, 2, 2));

    logger.debug("EXIT testSearch()");
  }

  @Test
  public void testGraph1() {
    logger.debug("ENTER testGraph1()");
    ServerProperties props = new ServerProperties();
    int n = 100000;
    int neighbors = 10;
    BitcoinFeaturesBase.rlogMemory();

    try {
      int max = 64;//128;
      for (int i = 32; i < max; i *=2) {
        long time1 = System.currentTimeMillis();
        BitcoinFeaturesBase.rlogMemory();
        logger.info(n + " and " + i + " -------------------- ");

        Map<Long, Integer> edgeToWeight = getGraph(n, i);

        Graph graph = new Graph(edgeToWeight);

        Runtime.getRuntime().gc();

        BitcoinFeaturesBase.rlogMemory();

        long time2 = new Date().getTime();
        logger.info("Time:" + (time2 - time1));
      }
    } catch (Exception e) {
      logger.error("got " +e,e);
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
  public void testGraph() {
    logger.debug("ENTER testSearch()");
    ServerProperties props = new ServerProperties();
    int n = 100000;
    int neighbors = 40;

    Map<Long, Integer> edgeToWeight = getGraph(n, neighbors);

    try {
      long time1 = System.currentTimeMillis();

      MultipleIndexConstructor.loadTypes(n);
      MultipleIndexConstructor.createTypedEdges();

      BitcoinFeaturesBase.logMemory();

      Graph graph = new Graph(edgeToWeight);

      BitcoinFeaturesBase.logMemory();

      MultipleIndexConstructor.populateSortedEdgeLists(graph);
      BitcoinFeaturesBase.logMemory();

      //load types file
  //    MultipleIndexConstructor.loadTypesFile();

      // Create Typed Edges

      // Load and Sort Edges from Graph
      //loadAndSortEdges();
      MultipleIndexConstructor.populateSortedEdgeLists(graph);
      BitcoinFeaturesBase.logMemory();

      //save the sorted edge lists
      MultipleIndexConstructor.saveSortedEdgeList();
      BitcoinFeaturesBase.logMemory();

      //test method that computes totalTypes
      // totalTypes = 0;
      MultipleIndexConstructor. computeTotalTypes();
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

      Set<Integer> types = new HashSet<>(1);
      types.add(1);
      MultipleIndexConstructor.makeTypeIDs(types);

      long then = System.currentTimeMillis();
      BitcoinFeaturesBase.rlogMemory();
      MultipleIndexConstructor.computeIndices(graph);
      BitcoinFeaturesBase.rlogMemory();

      long time2 = new Date().getTime();
      logger.info("Time:" + (time2 - time1));
      logger.info("Time to do computeIndices :" + (time2 - then));
    } catch (Exception e) {
      logger.error("got " +e,e);
    }

    sleep();

    logger.debug("EXIT testSearch()");
  }

  private Map<Long, Integer> getGraph(int n, int neighbors) {
    Map<Long, Integer> edgeToWeight = new HashMap<>();

    Random random = new Random();

    for (int from = 0; from < n; from++) {
      Set<Long> current = new HashSet<>();

      for (int j = 0; j < neighbors; j++) {
        long to = random.nextInt(n);
        while (to == from || current.contains(to)) {
          to = random.nextInt(n);
        }

        long l = BitcoinFeaturesBase.storeTwo(from, to);
        current.add(to);
    /*    int low = BitcoinFeaturesBase.getLow(l);
        int high = BitcoinFeaturesBase.getHigh(l);
        if (low != from) logger.error("huh?");
        if (high != to) logger.error("huh?");
    */
        int w = random.nextInt(10);
        edgeToWeight.put(l, w);
      }
    }

    logger.info("made " + edgeToWeight.size() + " with " + edgeToWeight.keySet().size() + " and " + edgeToWeight.values().size());
    return edgeToWeight;
  }
}

