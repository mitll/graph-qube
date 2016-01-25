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

import org.jgrapht.util.FibonacciHeap;
import org.jgrapht.util.FibonacciHeapNode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by go22670 on 1/11/16.
 */
public class ExtraGraph extends Graph {
  @SuppressWarnings("unchecked")

  /**
   * Maps node id to the number of times it appears in the changed edges
   * @see ExtraGraph#addMoreEdges(boolean, int)
   */
  private final HashMap<Integer, Integer> nodeFreqInNewEdges = new HashMap<>();


  /**
   * Performs a deep copy of the Graph to create a new graph
   */
/*
  public static Graph copyGraph(Graph g) {
    Graph g1 = new Graph();
    g1.setEdges(new HashSet<>());
    g1.setInLinks(new HashMap<>());
    for (Edge x : g.getEdges()) {
      g1.getEdges().add(x);
    }
    for (Integer i : g.getInLinks().keySet())
      g1.getInLinks().put(i, (ArrayList<Edge>) g.getInLinks().get(i).clone());
    return g1;
  }
*/
  public static void main(String[] args) throws Throwable {
    ExtraGraph g = new ExtraGraph();
//    g.setNumEdges(100);
//    g.setNumNodes(20);
    g.createRandomGraph(false);
    String randomFile = args[1];
    String graphFile = args[2];
    g.saveGraph(new File(args[0] + File.separator + randomFile));
    g.loadGraph(new File(args[0] + File.separator + graphFile));
  }

/*  public void findConnectedComponents(String baseDir, String graphFile) throws Throwable {
    loadGraph(new File(baseDir + File.separator + graphFile));
    HashSet<Integer> nodes = new HashSet<>();
    int count = 0;
    for (int i = 0; i < getNumNodes(); i++) {
      if (!nodes.contains(i)) {
        double dist[] = computeMinDistances(i);
        count++;
        nodes.add(i);
        System.out.print(" " + getRawID(i));
        for (int j = 0; j < dist.length; j++) {
          if (dist[j] != Double.MAX_VALUE && dist[j] != 0.) {
            nodes.add(j);
            System.out.print(" " + getRawID(j));
          }
        }
        System.out.println();
      }
    }
    System.out.println("Number of components = " + count);
  }*/

  /**
   * http://www.sciencedirect.com/science/article/pii/S0022000097915348
   *
   * @param maxNeeded
   * @param g
   * @throws Throwable
   */
  public void generateCohenLists(String baseDir, String graphFile, String cohenEstimatesFile) throws Throwable {
    double matrix[][];
    int maxNeeded = 10;
    int maxDist = 100;
    int cohenK = 50000;
    class CohenInternalNode {
      public CohenInternalNode(double d, double vk) {
        dist = d;
        nodeId = vk;
      }

      double nodeId;
      double dist;
    }
    double graphDiameter = 0;
    HashMap<Integer, Integer> rankMap = new HashMap<>();
    loadGraph(new File(baseDir + File.separator + graphFile));
    System.out.println("Generating lists");
    matrix = new double[getNumNodes()][maxDist];
    for (int k = 0; k < cohenK; k++) {
      if (k % 100 == 0)
        System.out.println(k);
      double ranks[] = new double[getNumNodes()];
      for (int m = 0; m < getNumNodes(); m++) {
        ranks[m] = Math.random();
      }
      Arrays.sort(ranks);
      ArrayList<ArrayList<CohenInternalNode>> list = new ArrayList<>();

      //reverse edge direction of graph
      //not required as we have an undirected graph
      double dist[] = new double[getNumNodes()];
      double infinity = 10000000.00;
      /* Initialization: set every distance to INFINITY until we discover a path */
      for (int j = 0; j < getNumNodes(); j++) {
        dist[j] = infinity;
      }
      for (int j = 0; j < getNumNodes(); j++) {
        list.add(j, new ArrayList<>());
      }
      //generatePermutations
      int ranks2[] = new int[getNumNodes()];
      for (int i = 0; i < getNumNodes(); i++) {
        ranks2[i] = i;
      }
      for (int k2 = ranks.length - 1; k2 > 0; k2--) {
        int w = (int) Math.floor(Math.random() * (k2 + 1));
        int temp = ranks2[w];
        ranks2[w] = ranks2[k2];
        ranks2[k2] = temp;
      }
      for (int k2 = 0; k2 < getNumNodes(); k2++)
        rankMap.put(ranks2[k2], k2);
      //process further
      for (int i = 0; i < getNumNodes(); i++) {
        int vi = rankMap.get(i);//sortedNodeIds[i];
        HashMap<Integer, FibonacciHeapNode<Integer>> heapedNodeToFHN = new HashMap<>();
        FibonacciHeap<Integer> heap = new FibonacciHeap<>();
        FibonacciHeapNode<Integer> fhn = new FibonacciHeapNode<>(vi, 0.0);
        heap.insert(fhn, 0.0);
        heapedNodeToFHN.put(fhn.getData(), fhn);
        while (!heap.isEmpty()) {
          FibonacciHeapNode<Integer> fhns = heap.removeMin();
          heapedNodeToFHN.remove(fhns.getData());
          int vk = fhns.getData();//minLableNodeId
          double D = fhns.getKey();//minLabel
          if (D > maxNeeded)
            break;
          list.get(vk).add(new CohenInternalNode(D, ranks[i]));
          if (D > graphDiameter)
            graphDiameter = D;
          dist[vk] = D;
          if (getInLinks().get(vk) != null) {
            for (Edge edge : getInLinks().get(vk)) { //reversed graph
              int vj = edge.getSrc();
              double weight = edge.getWeight();
              if (heapedNodeToFHN.containsKey(vj)) {
                FibonacciHeapNode<Integer> tempFHN = (FibonacciHeapNode<Integer>) heapedNodeToFHN.get(vj);
                double currentLabel = tempFHN.getKey();
                double newLabel = D + weight;
                if (newLabel < currentLabel) {
                  heap.decreaseKey(tempFHN, newLabel);
                }
              } else {
                if (D + weight < dist[vj]) {
                  FibonacciHeapNode<Integer> tempFHN = new FibonacciHeapNode<>(vj, D + weight);
                  heap.insert(tempFHN, tempFHN.getKey());
                  heapedNodeToFHN.put(tempFHN.getData(), tempFHN);
                }
              }
            }
          }
        }
      }
      for (int j = 0; j < getNumNodes(); j++) {
        ArrayList<CohenInternalNode> al = list.get(j);
        int last = maxDist - 1;
        for (CohenInternalNode node : al) {
          if (node.dist > maxDist - 1)
            continue;
          for (int i1 = last; i1 >= node.dist; i1--)
            matrix[j][i1] += node.nodeId;
          last = (int) (node.dist) - 1;
        }
      }
    }
    //print the estimates
    BufferedWriter out = new BufferedWriter(new FileWriter(new File(baseDir, cohenEstimatesFile)));
    // for (int j = 0; j < getNumNodes(); j++) {
    int j = 0;
    for (Integer nodeID : getRawIDs()) {
      out.write(nodeID + "#");
      for (int d = 0; d < maxDist; d++) {
        int val = (int) Math.round((double) cohenK / matrix[j][d]) - 1;
        out.write(val + ",");
      }
      out.write("\n");
      j++;
    }
    out.close();
  }

  /**
   * Computes shortest path distances on the graph considering `node' as the source node
   * using Dijkstra's algorithm
   *
   * @param node
   * @param graph
   * @return
   */
  private double[] computeMinDistances(int node) {
    double dist[] = new double[getNumNodes()];
    int prev[] = new int[getNumNodes()];
    FibonacciHeap<Integer> fh = new FibonacciHeap<>();
    /* Initialization: set every distance to INFINITY until we discover a path */
    for (int i = 0; i < getNumNodes(); i++) {
      dist[i] = Double.MAX_VALUE;
      prev[i] = -1;
    }

    HashMap<Integer, FibonacciHeapNode<Integer>> map = new HashMap<>();
    /* The distance from the source to the source is defined to be zero */
    dist[node] = 0;
    for (int i = 0; i < getNumNodes(); i++) {
      FibonacciHeapNode<Integer> fhn = new FibonacciHeapNode<>(i, dist[i]);
      fh.insert(fhn, dist[i]);
      map.put(i, fhn);
    }

    while (!fh.isEmpty()) {
      FibonacciHeapNode<Integer> u = fh.removeMin();
      if (u.getKey() == Double.MAX_VALUE)
        break;
      List<Edge> list = getInLinks().get(u.getData());
      if (list != null) {
        for (Edge edge : list) {
          int neighbor = edge.getSrc();
          double alt = dist[u.getData()] + edge.getWeight();
          if (alt < dist[neighbor]) {
            dist[neighbor] = alt;
            fh.decreaseKey(map.get(neighbor), alt);
            prev[neighbor] = u.getData();
          }
        }
      }
    }
    return dist;
  }

  /**
   * Computes shortest path distances from every node to every other node in Graph g.
   * Infinity and 0 distances are not printed.
   * Also distance from node a to node b is printed if a<b
   *
   * @param g
   */
/*  public void computeAllPairDistances() {
    for (int i = 0; i < getNumNodes(); i++) {
      double dist[] = computeMinDistances(i);
      for (int j = 0; j < getNumNodes(); j++) {
        if (dist[j] != Double.MAX_VALUE && dist[j] != 0. && getRawID(i) < getRawID(j))
          System.out.println(getRawID(i) + " " + getRawID(j) + " " + dist[j]);
      }
    }
  }*/

  /**
   * Computes graph diameter for a directed graph
   */
  public ArrayList<Object> computeGraphDiameter() {
    double max = 0;
    int node1 = -1;
    int node2 = -1;
    for (int i = 0; i < getNumNodes(); i++) {
      double dist[] = computeMinDistances(i);
      for (int j = 0; j < getNumNodes(); j++) {
        if (i < j && dist[j] != Double.MAX_VALUE) {
          if (dist[j] > max) {
            max = dist[j];
            node1 = i;
            node2 = j;
          }
        }
      }
    }
    ArrayList<Object> list = new ArrayList<>();
    list.add(node1);
    list.add(node2);
    list.add(max);
    return list;
  }

  /**
   * Removes edge from graph considering direction from a to b
   *
   * @param a
   * @param b
   * @param weight
   */
  private void removeEdge(int a, int b) {
    Edge e = null;
    List<Edge> al = new ArrayList<>();
    if (getInLinks().get(b) != null) {
      al = getInLinks().get(b);
    }
    for (Edge edge : al) {
      if (edge.getSrc() == a && edge.getDst() == b) {
        e = edge;
        break;
      }
    }
    al.remove(e);
    getInLinks().put(b, al);

    getEdges().remove(e);
  }

  /**
   * This method would remove existing edges from the current graph snapshot denoting the evolution of the graph by removal of edges.
   *
   * @param unitWeighted
   */
  public void removeEdges(boolean unitWeighted, int numEdgesToRemove) {
    for (int i = 0; i < numEdgesToRemove; i++) {
      int a = (int) (Math.random() * getNumNodes()) + 1;
      int b = (int) (Math.random() * getNumNodes()) + 1;
      if (a == b) {
        i--;
        continue;
      }
      if (this.getEdge(a - 1, b - 1) != null) {
        if (nodeFreqInNewEdges.containsKey(a - 1))
          nodeFreqInNewEdges.put(a - 1, nodeFreqInNewEdges.get(a - 1) + 1);
        else
          nodeFreqInNewEdges.put(a - 1, 1);
        if (nodeFreqInNewEdges.containsKey(b - 1))
          nodeFreqInNewEdges.put(b - 1, nodeFreqInNewEdges.get(b - 1) + 1);
        else
          nodeFreqInNewEdges.put(b - 1, 1);
        this.removeEdge(a - 1, b - 1);
        this.removeEdge(b - 1, a - 1);
    //    System.out.println("Edge deleted: " + getRawID(a - 1) + "--" + getRawID(b - 1));
      } else {
        i--;
      }
    }
  }

  /**
   * Creates a random graph with integer weights from 1 to 11.
   */
  public void createRandomGraphWithIntegerWeights() {
    for (int i = 0; i < getNumEdges(); i++) {
      int a = (int) (Math.random() * getNumNodes()) + 1;
      int b = (int) (Math.random() * getNumNodes()) + 1;
      if (a == b) {
        i--;
        continue;
      }
      double weight = (int) (Math.random() * 10) + 1;
      if (this.getEdge(a - 1, b - 1) == null) {
        this.addEdge(a - 1, b - 1, weight);
        this.addEdge(b - 1, a - 1, weight);
      } else {
        i--;
      }
    }
  }

  /**
   * Adds new edges to the graph to create a new snapshot (simulating the graph evolution)
   * Currently the new edges that are added are the ones that don't change the weight of the existing edges. i.e. Evolution here means that
   * new edges are added; old edges are not modified.
   *
   * @param unitWeighted
   * @param extraEdges
   */
  public void addMoreEdges(boolean unitWeighted, int extraEdges) {
    for (int i = 0; i < extraEdges; i++) {
      int a = (int) (Math.random() * getNumNodes()) + 1;
      int b = (int) (Math.random() * getNumNodes()) + 1;
      if (a == b) {
        i--;
        continue;
      }
      double weight = Math.random();
      if (unitWeighted)
        weight = 1;
      if (this.getEdge(a - 1, b - 1) == null) {
        if (nodeFreqInNewEdges.containsKey(a - 1))
          nodeFreqInNewEdges.put(a - 1, nodeFreqInNewEdges.get(a - 1) + 1);
        else
          nodeFreqInNewEdges.put(a - 1, 1);
        if (nodeFreqInNewEdges.containsKey(b - 1))
          nodeFreqInNewEdges.put(b - 1, nodeFreqInNewEdges.get(b - 1) + 1);
        else
          nodeFreqInNewEdges.put(b - 1, 1);
        this.addEdge(a - 1, b - 1, weight);
        this.addEdge(b - 1, a - 1, weight);
      //  System.out.println("New edge added: " + getRawID(a - 1) + "--" + getRawID(b - 1) + ":" + weight);
      } else {
        i--;
      }
    }
  }

  /**
   * Creates a random graph
   *
   * @param unitWeighted
   */
  private void createRandomGraph(boolean unitWeighted) {
    for (int i = 0; i < getNumEdges(); i++) {
      int a = (int) (Math.random() * getNumNodes()) + 1;
      int b = (int) (Math.random() * getNumNodes()) + 1;
      if (a == b) {
        i--;
        continue;
      }
      double weight = Math.random();
      if (unitWeighted)
        weight = 1.0;
      if (this.getEdge(a - 1, b - 1) == null) {
        this.addEdge(a - 1, b - 1, weight);
        this.addEdge(b - 1, a - 1, weight);
      } else {
        i--;
      }
    }
  }

  /**
   * Saves the graph to the file
   *
   * @param f
   * @throws Throwable
   */
  private void saveGraph(File f) throws Throwable {
    BufferedWriter out = new BufferedWriter(new FileWriter(f));
    out.write("#Randomly generated graph:\n");
    Date d = new Date();
    out.write("#Time: " + d + "\n");
    out.write("#Nodes: " + getNumNodes() + "\n");
    out.write("#Edges: " + getNumEdges() + "\n");
    out.write("#Undirected graph (each pair of nodes is saved twice) -- contains no self loops. #edges is #directed edges" + "\n");
    for (Edge edge : this.getEdges()) {
      out.write((edge.getSrc() + 1) + "#" + (edge.getDst() + 1) + "#" + edge.getWeight() + "\n");
    }
    out.close();
  }
}
