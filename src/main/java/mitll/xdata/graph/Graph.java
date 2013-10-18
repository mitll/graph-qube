package mitll.xdata.graph;

import influent.idl.FL_Entity;
import influent.idl.FL_Link;

import java.util.*;

/**
 * OK maybe we would want to use someone else's graph library, if it weren't too painful.
 *
 * User: go22670
 * Date: 6/27/13
 * Time: 4:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class Graph {
  private List<Edge> emptyList = Collections.emptyList();
  private List<Node> nodes= new ArrayList<Node>();

  public Graph(){}
  public void addNode(Node n) { nodes.add(n); }
  public Collection<Node> getNodes() {
    return nodes;
  }
  public String toString() { return "Graph of " + nodes.size() + " nodes";}
  public String print() {
    Set<Node> seen = new HashSet<Node>();
      StringBuilder builder = new StringBuilder();
    for (Node n : nodes) {
      if (seen.contains(n)) continue;
      seen.add(n);
      builder.append(n+"\n");
      Collection<Edge> edges = n.getEdges() == null ? emptyList : n.getEdges();
      for (Edge e : edges) {
        builder.append("\t" +e+"\n");
        seen.add(e.getSource());
        seen.add(e.getTarget());
      }
    }
    return builder.toString();
  }

  public void addNodes(Collection<Node> toAdd) {
    nodes.addAll(toAdd);
  }

  public static class Node {
    private String type;
    public Map<String,String> props;
    private List<Edge> edges;
    private String id;
    private FL_Entity entity;

    public Node(String id, String type, Map<String,String> props) { this.id = id; this.type = type; this.props = props; }
    public void addEdge(Edge edge) {
      if (edges == null) edges = new ArrayList<Edge>();
      edges.add(edge);

    }
    public Map<String,String>  getProps() { return props; }
    public List<Edge> getEdges() { return edges; }
    public String getType() { return type; }
    public String toString() {
      int size = edges == null ? 0 : edges.size();
      return "Node " + type + "/" + id+
          " : " + size + " edges";// +
   //       ", slots " + props.keySet();
    }

    public void setEntity(FL_Entity fl_entity) {
      this.entity = fl_entity;
    }

    /**
     * To make it easy to return results to Oculus
     * @return
     */
    public FL_Entity getEntity() { return entity; }
  }

  public static class Edge {
    String id;
    private FL_Link link;

    String type;
    private Node source;
    private Node target;
    Map<String,String> props;

    public Edge(String id, String type,Map<String,String> props) { this.id = id; this.type = type; this.props = props; }
    public Edge(String id, String type,Node source, Node target) { this.id = id; this.type = type; this.setSource(source); this.setTarget(target); }

    public Node getSource() {
      return source;
    }

    public void setSource(Node source) {
      this.source = source;
    }

    public Node getTarget() {
      return target;
    }

    /**
     * Sets the target entity uid of the target link
     * @param target
     */
    public void setTarget(Node target) {
      this.target = target;
      if (link != null) link.setTarget(target.getEntity().getUid());
    }

    public FL_Link getLink() {
      return link;
    }

    public void setLink(FL_Link link) {
      this.link = link;
    }

    public String toString() { return "Edge " + type + "/" +  id + " source " + getSource() + " -> target " +getTarget(); }
  }
}
