package uiuc.topksubgraph;

import java.util.Comparator;

/**
 * This class will represent an edge in the graph
 *
 * @author Manish Gupta (gupta58@illinois.edu)
 *         University of Illinois at Urbana Champaign
 */
public class Edge {
  private final int src;
  private final int dst;
  private double weight;

  /**
   * Constructor
   *
   * @param src
   * @param dst
   * @param weight
   */
  public Edge(int src, int dst, double weight) {
    this.src = src;
    this.dst = dst;
    this.setWeight(weight);
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

  public void setWeight(double weight) {
    this.weight = weight;
  }
}

class EdgeComparator implements Comparator<Edge> {
  @Override
  public int compare(Edge e1, Edge e2) {
    double e1wt = e1.getWeight();
    double e2wt = e2.getWeight();
    int comp = 0;
    if (e1wt > e2wt)
      comp = -1;
    else if (e1wt < e2wt)
      comp = 1;

    if (comp == 0) {
      comp = Integer.valueOf(e1.getSrc()).compareTo(e1.getSrc());
    }
    if (comp == 0) {
      comp = Integer.valueOf(e1.getDst()).compareTo(e1.getDst());
    }
    return comp;
  }
}