package uiuc.topksubgraph;

import java.util.Comparator;

/**
 * This class will represent an edge in the graph
 * @author Manish Gupta (gupta58@illinois.edu)
 * University of Illinois at Urbana Champaign
 */
public class Edge {
    public final int src, dst;
    public double weight;
    
    /**
     * Constructor
     * @param src
     * @param dst
     * @param weight
     */
    public Edge(int src, int dst, double weight) {
      this.src = src;
      this.dst = dst;
      this.weight = weight;
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + src + "->" + dst + ";" + weight + "]";
    }

    /**
     * Two edges are equal if they have source node, end node and weight.
     */
    @Override
    public boolean equals(Object obj) {
    	Edge oEdge = (Edge) obj;
        return oEdge.weight == weight && oEdge.src == src && oEdge.dst == dst;
    }
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
    	return (src*31)^dst^((int)(weight*1000000));
    }
}
class EdgeComparator implements Comparator<Edge>
{
	@Override
	public int compare(Edge e1, Edge e2) {
		double e1wt = e1.weight;        
        double e2wt = e2.weight;
        if(e1wt>e2wt)
        	return -1;
        else if(e1wt<e2wt)
        	return 1;
        else
        	return 0;
	}
}