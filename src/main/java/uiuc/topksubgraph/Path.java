package uiuc.topksubgraph;

import java.util.ArrayList;

/**
 * Represents a path which stores the path in the query and the instantiated graph node that represents the origin of the path.
 *
 * @author Manish Gupta (gupta58@illinois.edu)
 *         University of Illinois at Urbana Champaign
 */
class Path {
  public ArrayList<Long> nodes;
  private int u;

  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    Path p2 = (Path) obj;
    if (this.nodes.size() != p2.nodes.size())
      return false;
    int forward = 1;
    for (int i = 0; i < this.nodes.size(); i++)
      if ((!p2.nodes.get(i).equals(this.nodes.get(i))))
        forward = 0;
//		int backward=1;
//		for(int i=0;i<this.nodes.size();i++)
//			if((!p2.nodes.get(i).equals(this.nodes.get(this.nodes.size()-1-i))))
//				backward=0;
    if (forward == 1)//||backward==1)
      return true;
    else
      return false;
  }

  Path() {
    nodes = new ArrayList<>();
  }

  Path copyPath() {
    Path p = new Path();
    p.nodes = new ArrayList<>();
    for (long n : nodes)
      p.nodes.add(n);
    p.u = u;
    return p;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int hash = u;
    for (long i : nodes)
      hash = hash ^ (int) i;
    return hash;
  }
}
