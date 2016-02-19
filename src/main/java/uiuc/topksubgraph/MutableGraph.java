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

package uiuc.topksubgraph;

import mitll.xdata.dataset.bitcoin.features.BitcoinFeaturesBase;
import mitll.xdata.db.DBConnection;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.*;

/**
 * Created by go22670 on 2/19/16.
 */
public class MutableGraph extends Graph {
  private static final Logger logger = Logger.getLogger(MutableGraph.class);

  public MutableGraph() {
  }


  public MutableGraph(Map<Long, Integer> edgeToWeight) {
    this(edgeToWeight, false);
//    this.populateInLinks2 = populateInLinks2;
    //   simpleIds2(edgeToWeight);
    //   loadGraphFromMemory(edgeToWeight);
  }

  /**
   * Loads the graph...
   *
   * @throws SQLException
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#computeIndicesFromMemory(String, DBConnection, Map)
   */
  public MutableGraph(Map<Long, Integer> edgeToWeight, boolean populateInLinks2) {
    logger.info("loadGraphFromMemory map " + 0 + " vs " + edgeToWeight.size());

    this.populateInLinks2 = populateInLinks2;

    int c = 0;
    for (Map.Entry<Long, Integer> edgeAndCount : edgeToWeight.entrySet()) {
      Long key = edgeAndCount.getKey();

      int from = BitcoinFeaturesBase.getLow(key);
      int to = BitcoinFeaturesBase.getHigh(key);
      int weight = edgeAndCount.getValue();

      if (c++ % 1000000 == 0) {
        logger.debug("loadGraphFromMemory read  " + c + " : " + BitcoinFeaturesBase.getMemoryStatus());
      }
      addNodeAndEdge(from, to, weight);
    }
  }



  /**
   * Adds edge to graph considering direction from a to b
   *
   * @param a
   * @param b
   * @param weight
   */
  protected void addEdge(int a, int b, double weight) {
    Edge e = new Edge(a, b, weight);
    if (!edges.contains(e)) {
      edges.add(e);
//       logger.info("addEdge adding " + e);
      List<Edge> al = inLinks.get(b);

      if (inLinks.get(b) == null) {
        al = new ArrayList<>();
        inLinks.put(b, al);
      }

      al.add(e);

      if (populateInLinks2) {
        Map<Integer, Edge> integerEdgeMap = inLinks2.get(b);
        if (integerEdgeMap == null) {
          integerEdgeMap = new HashMap<>();
          inLinks2.put(b, integerEdgeMap);
        }

        integerEdgeMap.put(a, e);
      }
    }
  }

  /**
   * Loads the graph from an h2 database
   *
   * @param dbConnection
   * @throws Throwable
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#computeIndices(String, DBConnection)
   * @deprecated
   */
  public MutableGraph(DBConnection dbConnection, String tableName, String edgeName) throws SQLException {
    loadGraph(dbConnection.getConnection(), tableName, edgeName);
  }

  /**
   * Loads the graph from an h2 database
   *
   * @param connection
   * @param tableName
   * @throws SQLException
   * @see #loadGraph(DBConnection, String, String)
   */
  public MutableGraph(Connection connection, String tableName) throws SQLException {
    // int nodeCount = node2NodeIdMap.size();
//    setNumNodes(0);
//    setNumEdges(0);

		/*
     * Do database query
		 */
    String sqlQuery = "select * from " + tableName;

    logger.info("loadGraphAgain doing " + sqlQuery);
    PreparedStatement queryStatement = connection.prepareStatement(sqlQuery);
    ResultSet rs = queryStatement.executeQuery();

		/*
     * Loop through database table rows...
		 */
    int c = 0;
    while (rs.next()) {
      c++;
      if (c % 1000000 == 0) logger.debug("read  " + c);

      int i = 1;
      int from = rs.getInt(i++);
      int to = rs.getInt(i++);
      int weight = rs.getInt(i++);

/*      if (node2NodeIdMap.containsKey(from))
        from = node2NodeIdMap.get(from);
      else {
        node2NodeIdMap.put(from, nodeCount);
        nodeId2NodeMap.put(nodeCount, from);
        from = nodeCount;
        nodeCount++;
      }
      if (node2NodeIdMap.containsKey(to))
        to = node2NodeIdMap.get(to);
      else {
        node2NodeIdMap.put(to, nodeCount);
        nodeId2NodeMap.put(nodeCount, to);
        to = nodeCount;
        nodeCount++;
      }*/

      addNodeAndEdge(from, to, weight);
    }

    rs.close();
    queryStatement.close();

//    setNumNodes(nodeIds.size());
//    setNumEdges(c);

    logger.info("read " + c + " found " + getNumNodes() + " nodes and " + getNumEdges() + " edges");
  }

  /**
   * Loads the graph from an h2 database
   *
   * @param connection
   * @param tableName
   * @param edgeName
   * @throws SQLException
   * @see #loadGraph
   */
  private void loadGraph(Connection connection, String tableName, String edgeName) throws SQLException {
    // int nodeCount = node2NodeIdMap.size();
//    setNumNodes(0);
//    setNumEdges(0);

		/*
     * Do database query
		 */
    //Connection connection = dbConnection.getConnection();

    String sqlQuery = "select * from " + tableName + ";";

    PreparedStatement queryStatement = connection.prepareStatement(sqlQuery);
    ResultSet rs = queryStatement.executeQuery();

		/*
     * Loop through database table rows...
		 */
    int c = 0;
    while (rs.next()) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " + c);

      Array array = rs.getArray("sorted_pair");
      Object rsArray = array.getArray();
      Object[] sortedPair = (Object[]) rsArray;

      double weight = rs.getDouble(edgeName);

//			double tot_out = rs.getDouble("tot_out");
//			double tot_in = rs.getDouble("tot_in");
//			double tot_usd = rs.getDouble("tot_usd");
//			double num_trans = rs.getDouble("num_trans");
//			weight = tot_usd/num_trans;
//			weight = 1;

      int from = (Integer) sortedPair[0];
      int to = (Integer) sortedPair[1];
//			long from = (Long)sortedPair[0];
//			long to = (Long)sortedPair[1];

      //logger.info("First node is: "+from+" Second node is: "+to+" Number of trans: "+weight);

/*      if (node2NodeIdMap.containsKey(from))
        from = node2NodeIdMap.get(from);
      else {
        node2NodeIdMap.put(from, nodeCount);
        nodeId2NodeMap.put(nodeCount, from);
        from = nodeCount;
        nodeCount++;
      }
      if (node2NodeIdMap.containsKey(to))
        to = node2NodeIdMap.get(to);
      else {
        node2NodeIdMap.put(to, nodeCount);
        nodeId2NodeMap.put(nodeCount, to);
        to = nodeCount;
        nodeCount++;
      }*/

//			if (tot_out == 0.0) {this.addEdge(to,from,weight);}
//			if (tot_in == 0.0) {this.addEdge(from,to,weight);}
//
//			if (tot_out != 0.0 && tot_in != 0.0) {
//				this.addEdge(from, to, weight);
//				this.addEdge(to, from, weight);
//			}

      addNodeAndEdge(from, to, weight);
    }

    rs.close();
    queryStatement.close();

//    setNumNodes(nodeIds.size());
//    setNumEdges(c);
  }

  private void addNodeAndEdge(int from, int to, double weight) {
    nodeIds.add(from);
    nodeIds.add(to);
    this.addEdge(from, to, weight);
    this.addEdge(to, from, weight);
  }

  /**
   * Loads the graph from a HashSet of edges
   *
   * @param edges
   */
  public MutableGraph(Collection<Edge> edges) {
//    int nodeCount = node2NodeIdMap.size();
//    setNumNodes(0);
//    setNumEdges(0);

		/*
     * Loop through edges...
		 */
    int c = 0;
    for (Edge edg : edges) {
      c++;
      if (c % 100000 == 0) logger.debug("read  " + c);

      //get edge information
      int from = edg.getSrc();
      int to = edg.getDst();
      double weight = edg.getWeight();

      //logger.info("First node is: "+from+" Second node is: "+to+" Number of trans: "+weight);

/*      if (node2NodeIdMap.containsKey(from))
        from = node2NodeIdMap.get(from);
      else {
        node2NodeIdMap.put(from, nodeCount);
        nodeId2NodeMap.put(nodeCount, from);
        from = nodeCount;
        nodeCount++;
      }
      if (node2NodeIdMap.containsKey(to))
        to = node2NodeIdMap.get(to);
      else {
        node2NodeIdMap.put(to, nodeCount);
        nodeId2NodeMap.put(nodeCount, to);
        to = nodeCount;
        nodeCount++;
      }*/

      addNodeAndEdge(from, to, weight);
    }

//    setNumNodes(nodeCount);
//    setNumEdges(c);

    report();
  }

  /**
   * Loads the graph from a file
   *
   * @param f
   * @throws Throwable
   */
  public MutableGraph(File f) throws Throwable {
    BufferedReader in = new BufferedReader(new FileReader(f));
    String str = "";
//    int nodeCount = node2NodeIdMap.size();
    while ((str = in.readLine()) != null) {
      if (str.startsWith("#")) {
        if (str.contains("#Nodes")) {
//          setNumNodes(Integer.parseInt(str.split("\\s+")[1].trim()));
//          setNumEdges(Integer.parseInt(in.readLine().split("\\s+")[1].trim()));
        }
        //System.err.println(str);
        continue;
      }
      String tokens[] = str.split("#|\\t");
      int from = Integer.parseInt(tokens[0]);
      int to = Integer.parseInt(tokens[1]);
/*      if (node2NodeIdMap.containsKey(from))
        from = node2NodeIdMap.get(from);
      else {
        node2NodeIdMap.put(from, nodeCount);
        nodeId2NodeMap.put(nodeCount, from);
        from = nodeCount;
        nodeCount++;
      }
      if (node2NodeIdMap.containsKey(to))
        to = node2NodeIdMap.get(to);
      else {
        node2NodeIdMap.put(to, nodeCount);
        nodeId2NodeMap.put(nodeCount, to);
        to = nodeCount;
        nodeCount++;
      }*/
      if (tokens.length > 2) {
        this.addEdge(from, to, Double.parseDouble(tokens[2]));
        this.addEdge(to, from, Double.parseDouble(tokens[2]));
      } else {
        this.addEdge(from, to, 1);
        this.addEdge(to, from, 1);
      }
    }
    in.close();
  }

/*  public void simpleIds2(Map<Long, Integer> edgeToWeight) {
    //Map<Integer,Integer> extToInternal = new HashMap<>();
    int count = 0;
    for (Long key : edgeToWeight.keySet()) {
      int from = BitcoinFeaturesBase.getLow(key);
      int to = BitcoinFeaturesBase.getHigh(key);

*//*      Integer finternal = node2NodeIdMap.get(from);
      if (finternal == null) {
        finternal = count++;
        node2NodeIdMap.put(from, finternal);
        nodeId2NodeMap.put(finternal, from);
      }

      Integer tinternal = node2NodeIdMap.get(to);
      if (tinternal == null) {
        tinternal = count++;
        node2NodeIdMap.put(to, tinternal);
        nodeId2NodeMap.put(tinternal, to);
      }*//*

      nodeIds.add(from);
      nodeIds.add(to);

    }
  }*/
}
