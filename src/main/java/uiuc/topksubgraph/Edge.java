package uiuc.topksubgraph;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

/**
 * This class will represent an edge in the graph
 *
 * @author Manish Gupta (gupta58@illinois.edu)
 *         University of Illinois at Urbana Champaign
 */
public class Edge {
  private final int src;
  private final int dst;
  private float weight;

  /**
   * Constructor
   *
   * @param src
   * @param dst
   * @param weight
   * @see Graph#addEdge(int, int, double)
   * @see MultipleIndexConstructor#populateSortedEdgeLists(Graph)
   * @see QueryExecutor#getUpperbound(HashSet, ArrayList)
   */
  public Edge(int src, int dst, double weight) {
    this.src = src;
    this.dst = dst;
    this.weight = (float) weight;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + getSrc() + "->" + getDst() + ";" + getWeight() + "]";
  }

  /**
   * Two edges are equal if they have source node, end node and weight.
   */
  @Override
  public boolean equals(Object obj) {
    Edge oEdge = (Edge) obj;
    return oEdge.getSrc() == getSrc() && oEdge.getDst() == getDst() && oEdge.getWeight() == getWeight();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return (getSrc() * 31) ^ getDst() ^ ((int) (getWeight() * 1000000));
  }

  public int getSrc() {
    return src;
  }

  public int getDst() {
    return dst;
  }

  public double getWeight() {
    return weight;
  }

  public float getFWeight() {
    return weight;
  }

//  public void setWeight(double weight) {
//    this.weight = weight;
//  }
}