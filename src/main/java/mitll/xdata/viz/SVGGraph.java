package mitll.xdata.viz;

import influent.idl.*;
import mitll.xdata.binding.Binding;
import org.apache.log4j.Logger;
import org.gephi.graph.api.*;
import org.gephi.io.exporter.api.ExportController;
import org.gephi.io.exporter.preview.SVGExporter;
import org.gephi.layout.plugin.scale.ScaleLayout;
import org.gephi.preview.api.PreviewController;
import org.gephi.preview.api.PreviewModel;
import org.gephi.preview.api.PreviewProperty;
import org.gephi.preview.types.DependantOriginalColor;
import org.gephi.preview.types.EdgeColor;
import org.gephi.project.api.ProjectController;
import org.openide.util.Lookup;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.awt.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: GO22670
 * Date: 7/17/13
 * Time: 1:47 PM
 * To change this template use File | Settings | File Templates.
 */
public class SVGGraph {
  public static final int SVG_WIDTH_PCT = 60;
  public static final long HOUR_MILLIS = (3600l * 1000l);
  public static final int CANVAS_WIDTH = 1000;
  public static final int CANVAS_HEIGHT = 300;
  public static final int VIEW_BOX_WIDTH = 1200;
  public static final int VIEW_BOX_HEIGHT = 400;  //"-16 -3 66 4"    // "-16 -8 66 16"
  public static final int SVG_HEIGHT_PCT = 50;
  int heightPCT = 40;

  public static final int WIDTH_PCT = 60;
  private static Logger logger = Logger.getLogger(SVGGraph.class);

 // public static final int RIGHT_COLUMN = 45;
  public static final int MARGIN = 40;
  public static final int INITIAL_Y = 12;
  public static final int LEFT_COLUMN_X = 2;
  public static final int X_COLUMN_STEP = 30;
  public static final int Y_STEP = 5;
  public static final int MAX_TEXT_LENGTH = 15;

  //public static final float OPTIMAL_DISTANCE = 50f;

  /**
   * TODO : show stimulus graph?
   * TODO : show directed edges?
   * TODO : show scores with each graph?
   * TODO : better margins...?
   *
   * @see mitll.xdata.viz.SVGGraph#toSVG
   * @param results
   * @param writer
   */
  private void toSVG(FL_PatternSearchResults results, Writer writer) {
    ProjectController pc = Lookup.getDefault().lookup(ProjectController.class);
    pc.newProject();

    GraphController lookup = Lookup.getDefault().lookup(GraphController.class);
    GraphModel model = lookup.getModel();
    DirectedGraph dag = model.getDirectedGraph();

    int y = INITIAL_Y;

    for (FL_PatternSearchResult result : results.getResults()) {
      if (result.getLinks().isEmpty()) y = 2;
    }

    logger.debug("got  " + results.getResults().size() + " results");

    for (FL_PatternSearchResult result : results.getResults()) {
      int maxY = convertOneGraph(model, dag, y, result);

      y = maxY;
      y += 2 * Y_STEP;
    }
    //createSampleData();
    //doLayout(model);


    customizeApperance();

    ExportController ec = Lookup.getDefault().lookup(ExportController.class);
    SVGExporter svg = (SVGExporter) ec.getExporter("svg");
    ec.exportWriter(writer, svg);

  }

  /**
   * @see #toSVG(java.util.List, influent.idl.FL_PatternSearchResults, mitll.xdata.binding.Binding)
   * @param results
   * @param writer
   * @param binding
   * @throws Exception
   */
  private void toSVG2(List<FL_EntityMatchDescriptor> queryEntities, FL_PatternSearchResults results, Writer writer, Binding binding) throws Exception {

	  int count = 0;
	  

	  /*
	   * Write-out resultant subgraphs
	   */
	  for (FL_PatternSearchResult result : results.getResults()) {
		  ProjectController pc = Lookup.getDefault().lookup(ProjectController.class);
		  pc.newProject();
		  GraphController lookup = Lookup.getDefault().lookup(GraphController.class);
		  GraphModel model = lookup.getModel();
		  // model.clear();
		  DirectedGraph dag = model.getDirectedGraph();
		  // dag.clear();
		  int edgeCount = dag.getEdgeCount();

		  logger.debug("edges " + edgeCount);
		  logger.debug("nodes " + dag.getNodeCount());

		  int y = 1;

		  logger.debug("got  " + results.getResults().size() + " results");

		  /*int maxY =*/
		  convertOneGraph(model, dag, y, result); //lays out the graph...

		  //writer.write(toSVG3(dag));

		  //  y = maxY;
		  //  y += 2 * Y_STEP;
		  //}
		  //createSampleData();
		  //doLayout(model);
		  if (count == 0)   //first result will be the query...
			  writer.write("<h2>Query Subgraph</h2>\n");
		  else if (count == 1)
			  writer.write("<h2>Results</h2>\n");

		  writer.write("<div " +
				  //"width='50%'" +
				  " " +
				  (count++ % 2 == 0 ? "style='background-color:" +
						  //  "#b0c4de" +
						  //"#cedcd7"+
						  "#f2f2f2" +
						  ";'" : "") +
				  ">\n");
		  if (true) {
			  String newXML = getSVGForGraph();
			  //   String newXML = svgXML;

			  writer.write("score: "+result.getScore()+"\n");
			  writer.write(newXML + "\n");
		  }
		  writer.write("\n");

		  /*
		   * Get list of entities where ORDER MATTERS...
		   */
		  List<FL_EntityMatchResult> entities = result.getEntities();
		  List<String> idsList = new ArrayList<String>(entities.size());
		  for (int i=0; i<entities.size(); i++) {
			  idsList.add("");
		  }
		  for (FL_EntityMatchResult entity : entities) {
			  int entityInd = Integer.parseInt(entity.getUid().split("E")[1]);
			  String entityName = entity.getEntity().getUid();
			  idsList.set(entityInd, entityName);
		  }

		  List<Binding.Edge> edgesForResult = binding.getEdgesForResult(result);
		  Collections.sort(edgesForResult);
		  writer.write(toTimePlot(edgesForResult,idsList));
		  writer.write("</div>");

		  try {
			  writer.write("<p></p>\n");
		  } catch (IOException e) {
			  e.printStackTrace();
		  }
		  pc.closeCurrentProject();
	  }
  }

  /**
   * Use gephi to convert a graph into an svg rendering.
   * @see #toSVG2(influent.idl.FL_PatternSearchResults, java.io.Writer, mitll.xdata.binding.Binding)
   * @return
   */
  private String getSVGForGraph() {
    customizeApperance();

    ExportController ec = Lookup.getDefault().lookup(ExportController.class);
    //logger.debug("ec " + ec);

    SVGExporter svg = (SVGExporter) ec.getExporter("svg");
    //logger.debug("svg " + svg);
//      svg.getWorkspace().getLookup();


    StringWriter internal = new StringWriter();
    ec.exportWriter(internal, svg);

    String svgXML = internal.toString();
    if (svgXML.indexOf("<svg") != 0) {
      logger.debug("trimming doctype! ---> starting at " + svgXML.indexOf("<svg"));
      svgXML = svgXML.substring(svgXML.indexOf("<svg"));
    }
    return fixDimensions(svgXML);
  }

  /**
   * <svg version="1.1" xmlns="http://www.w3.org/2000/svg" preserveAspectRatio="xMidYMid meet" height="841px" viewBox="-13 -16 59 21" contentStyleType="text/css" zoomAndPan="magnify" xmlns:xlink="http://www.w3.org/1999/xlink" width="2362px" contentScriptType="text/ecmascript">
   <g id="edges">
   <path class="25 26" stroke="#808080" stroke-opacity="1.0" d="M 21.204346,-1.000000 C 21.363476,-5.159131 27.840870,-11.159131 32.000000,-11.000000" stroke-width="0.1" fill="none">
   <path class="25 27" stroke="#808080" stroke-opacity="1.0" d="M 21.204346,-1.000000 C 23.363476,-3.159131 29.840870,-3.159131 32.000000,-1.000000" stroke-width="0.1" fill="none">
   </g>
   <g id="nodes">
   <circle class="25" stroke-width="1.0" stroke-opacity="0.2" stroke="#000000" cy="-1.0" cx="21.204346" r="1.0" fill="#999999" fill-opacity="0.2">
   <circle class="26" stroke-width="1.0" stroke-opacity="0.2" stroke="#000000" cy="-11.0" cx="32.0" r="1.0" fill="#999999" fill-opacity="0.2">
   <circle class="27" stroke-width="1.0" stroke-opacity="0.2" stroke="#000000" cy="-1.0" cx="32.0" r="1.0" fill="#999999" fill-opacity="0.2">
   </g>
   <g id="node-labels">
   <g id="edge-labels">
   </svg>

   <g id="node-labels">
   <text class="n0" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#0000ff" y="-0.6345215" x="437.21198" font-size="2"> 5249540 : 100 </text>
   <text class="n0" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#0000ff" y="-18.634521" x="32.0" font-size="2"> 5249569 : 100 </text>
   <text class="n0" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#0000ff" y="-6.6342773" x="32.0" font-size="2"> 5249535 : 100 </text>
   <text class="n0" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#0000ff" y="-0.6345215" x="32.0" font-size="2"> 5249497 : 100 </text>
   <text class="n0" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#0000ff" y="-12.6345215" x="257.154" font-size="2"> 11 : 100 </text>
   </g>
   <g id="edge-labels">
   <text class="e5" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#808080" y="50.7818" x="231.90598" font-size="2"> 1/145/0/145/ </text>
   <text class="e1" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#808080" y="56.781796" x="233.706" font-size="2"> 1/21/0/21/ </text>
   <text class="e0" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#808080" y="59.7818" x="234.60599" font-size="2"> 1/848/0/848/ </text>
   <text class="e4" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#808080" y="-49.7731" x="145.47699" font-size="2"> 1/111/0/111/ </text>
   <text class="e3" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#808080" y="-43.7731" x="143.677" font-size="2"> 1/16/0/16/ </text>
   <text class="e2" font-family="Arial" style="text-anchor: middle; dominant-baseline: central;" fill="#808080" y="-40.7731" x="142.77701" font-size="2"> 1/650/0/650/ </text>
   </g>
   */
  private String toSVG3(Graph graph) {
    String header = "<svg version=\"1.1\" " +
        "xmlns=\"http://www.w3.org/2000/svg\" " +
        "preserveAspectRatio=\"xMidYMid meet\" " +
        //  "height=\"841px\" " +
          "height=\"50%\" " +
      //  "viewBox=\"-13 -16 59 21\" " +
        "contentStyleType=\"text/css\" " +
        "zoomAndPan=\"magnify\" " +
        "xmlns:xlink=\"http://www.w3.org/1999/xlink\" " +
        //  "width=\"2362px\" " +
          "width=\"50%\" " +
        "contentScriptType=\"text/ecmascript\">\n" +
        "   ";
    String edges = "   <g id=\"edges\">\n";
    for (Edge e : graph.getEdges()) {
      float x1 = e.getSource().getNodeData().x();
      float y1 = e.getSource().getNodeData().y();
      float x2 = e.getTarget().getNodeData().x();
      float y2 = e.getTarget().getNodeData().y();
      edges += "   <line " +
          //"class=\"25 26\" " +
          "stroke=\"#808080\" stroke-opacity=\"1.0\" " +
          //  "d=\"M 21.204346,-1.000000 C 21.363476,-5.159131 27.840870,-11.159131 32.000000,-11.000000\" " +
          "x1=" + x1 + " " +
          "y1=" + y1 + " " +
          "x2=" + x2 + " " +
          "y2=" + y2 + " " +
          "stroke-width=\"0.1\" fill=\"none\"></line>\n";

      edges += "   <text " +
          //"class=\"e5\" " +
          "font-family=\"Arial\" " +
          "style=\"text-anchor: middle; dominant-baseline: central;\" fill=\"#808080\" " +
          "y=\"" +
          (y1+y2)/2 +
          "\" " +
          "x=\"" +
          (x1+x2)/2 +
          "\" " +
          "font-size=\"2\"" +
          "> " +
          e.getEdgeData().getLabel() +
          " </text>\n";
    }
    edges += "</g>";

    String nodes = "<g id='nodes'>\n";
    for (Node n : graph.getNodes()) {
            nodes += "   <circle " +
               // "class=\"25\"" +
                " stroke-width=\"1.0\" stroke-opacity=\"0.2\" stroke=\"#000000\" " +
                "cy=\"" +
                "-" +
                n.getNodeData().y() +
                "\" " +
                "cx=\"" +
                n.getNodeData().x() +
                "\" " +
                "r=\"1.0\" " +
                "fill=\"#999999\" fill-opacity=\"0.2\"></circle>\n";

      nodes += "   <text " +
          "class=\"n0\" " +
          "font-family=\"Arial\" style=\"text-anchor: middle; dominant-baseline: central;\" fill=\"#0000ff\" " +
          "y=\"" +
          "-" +
          n.getNodeData().y() +
          "\" " +
          "x=\"" +
          n.getNodeData().x() +
          "\" font-size=\"2\">" +
          n.getNodeData().getLabel() +
          "</text>\n";
    }
    nodes+= "</g>";

    return header + edges + nodes + "</svg>";

  }

  /**
   * @see #toSVG2(influent.idl.FL_PatternSearchResults, java.io.Writer, mitll.xdata.binding.Binding)
   * @param edgeList
   * @return
   */
  private String toTimePlot(Collection<Binding.Edge> edgeList,List<String> ids) {
    logger.debug("making time plot for " + edgeList.isEmpty() + " edges----->");

    String viewLeft = "-120";
    String header = "<svg version=\"1.1\" " +
        "xmlns=\"http://www.w3.org/2000/svg\" " +
        "preserveAspectRatio=\"xMidYMid meet\" " +
        //  "height=\"841px\" " +
        "height=\"" +
        heightPCT +
        "%\" " +
          "viewBox=\"" +
        viewLeft +
        " -20 " +
        VIEW_BOX_WIDTH +
        " " +
        VIEW_BOX_HEIGHT +
        "\" " +
        "contentStyleType=\"text/css\" " +
        "zoomAndPan=\"magnify\" " +
        "xmlns:xlink=\"http://www.w3.org/1999/xlink\" " +
        //  "width=\"2362px\" " +
        "width=\"" +
        WIDTH_PCT +
        "%\" " +
        "contentScriptType=\"text/ecmascript\">\n" +
        "   ";
   // String edges = "   <g id=\"edges\">\n";

    long min = Long.MAX_VALUE;
     long max = Long.MIN_VALUE;

    Set<String> users = new HashSet<String>();
    List<String> inOrder = new ArrayList<String>();
    for (Binding.Edge e : edgeList) {
    	// logger.debug("source = " + e.getSource() + ", target = " + e.getTarget());
      if (e.getTime() > max) max = e.getTime();
      if (e.getTime() < min) min = e.getTime();
      String stringSource = "" + e.getSource();
      if (!users.contains(stringSource)) inOrder.add(stringSource);
      String stringTarget = "" + e.getTarget();
      if (!users.contains(stringTarget)) inOrder.add(stringTarget);
      users.add(stringSource); users.add(stringTarget);
      //inOrder.add(e.getSource());
    }
    if (max == min) {
      min -= HOUR_MILLIS;
      max = min + 2*HOUR_MILLIS;
    }

    long timeWindow = max-min;


    logger.debug("window " + timeWindow + " users " + users);

    String xml = "";
    long hours = timeWindow/ HOUR_MILLIS;
    String unit = "hours";
    if (hours > 1000) {
      hours = timeWindow/(24*HOUR_MILLIS);
      unit = "days";
    }

    if (hours < 1) {
      hours = timeWindow/(60*1000);
      unit = "minutes";
    }
    if (hours < 1) {
      hours = timeWindow/(1000);
      unit = "seconds";
    }
   /// int vStep = 10;
    long width = CANVAS_WIDTH;
    int height = CANVAS_HEIGHT;
   double wHours = timeWindow == 0 ? width/2 : (double)width/(double)timeWindow;
    long hUsers = (height-10)/(users.size());

    String leftAxis   = "<line " +
        "stroke=\"#808080\" stroke-opacity=\"1.0\" " +

        "x1='-6' y1='0' x2='-6' y2='"+(height) +
        "' stroke-width=\"6\" fill=\"none\"" +
        " />\n";
    String bottomAxis = "<line x1='-6' y1='" +(height)+
        "' x2='" + (width)+
        "' y2='"+(height) +

        "' stroke=\"#808080\" stroke-opacity=\"1.0\" " +
        " stroke-width=\"6\" fill=\"none\"" +

        " />\n";

    String marker = "<defs><marker id=\"mTriangle\" markerWidth=\"5\" markerHeight=\"10\"\n" +
        "        refX=\"5\" refY=\"5\" orient=\"auto\">\n" +
        "        <path d=\"M 0 0 5 5 0 10 Z\" style=\"fill: black;\"/>\n" +
        "    </marker></defs>";
   // xml += marker;

    xml += header +marker+ leftAxis + bottomAxis;

    inOrder = ids;
    logger.info("ids: "+ids);
    for (String user : inOrder) {
       xml += "<text x='-100' y='"+((inOrder.indexOf(user)*hUsers) + 10)+
           "' font-size=\"20\"" +
           " >" + user+
           "</text>";
    }

    int timeStep = 8;
    String last = "";
    long timeLabelStep = timeWindow / timeStep;
    if (timeLabelStep == 0) {
      min -= HOUR_MILLIS;
      max = min + 2*HOUR_MILLIS;
      timeLabelStep = (max-min)/2;
    }
    for (long i = min; i < max; i += timeLabelStep) {
      SimpleDateFormat sdf = unit.equals("days") ? new SimpleDateFormat("yyyy-MM-dd") : new SimpleDateFormat("yyyy-MM-dd hh:mm");
      String format = sdf.format(new Date(i));

     // if (format.equals(last)) format = format.substring(last.length());
      double x1 = //10 + 1 +
          ((i-min)*wHours);

      String toShow = format;
      if (last.length() > 0 && format.substring(0, "yyyy-MM-dd".length()).equals(last.substring(0, "yyyy-MM-dd".length()))) {
        toShow = toShow.substring("yyyy-MM-dd ".length());
      }
      else if (last.length() > 0 && format.substring(0, "yyyy-MM".length()).equals(last.substring(0, "yyyy-MM".length()))) {
        toShow = toShow.substring("yyyy-MM-".length());
      }
      else if (last.length() > 0 && format.substring(0, "yyyy".length()).equals(last.substring(0, "yyyy".length()))) {
        toShow = toShow.substring("yyyy-".length());
      }
      last = format;
     // logger.debug("toShow '" + toShow + "' format " +format + " at " + x1);

      int fontSize = 15;
      xml += "<text x='" +
          (x1-20) +
          "' y='" + (height + 20) +
          "' font-size=\"" +
          fontSize +
          "\"" +
          " >" + toShow +
          "</text>";

      // add tic
      xml+=

      "<line x1='" +x1+
          "' y1='" +((height )-6)+
          "' x2='" + x1+
          "' y2='"+((height )+6)+

          "' stroke=\"#808080\" stroke-opacity=\"1.0\" " +
          " stroke-width=\"2\" fill=\"none\"" +

          " />";
    }
    logger.warn("doing " + edgeList.size() + " edges");

    for (Binding.Edge e : edgeList) {
      int sIndex = inOrder.indexOf("" + e.getSource());
      int tIndex = inOrder.indexOf("" + e.getTarget());

      long y1 = 10+(sIndex*hUsers);
      long y2 = 10+(tIndex*hUsers);
      double x1 =
          //10 + 1 +
              ((e.getTime()-min)*wHours);
      x1 = Math.round(x1);

      boolean down = y2 > y1;
      xml +=  "   <line " +
          //"class=\"25 26\" " +
          "stroke=\"" +
          (down ? "blue" : "green")+//"#808080" +
          "\" stroke-opacity=\"1.0\" " +
          //  "d=\"M 21.204346,-1.000000 C 21.363476,-5.159131 27.840870,-11.159131 32.000000,-11.000000\" " +
          "x1='" + x1 + "' " +
          "y1='" + y1 + "' " +
          "x2='" + x1 + "' " +
          "y2='" + y2 + "' " +
          "stroke-width=\"" +
          2 +
          "\" fill=\"none\" " +
          " marker-end=\"url(#mTriangle)\" "+      //   marker-end="url(#Triangle)"
          "></line>\n";

    }

    return xml+ "</svg>";

  }


  private String fixDimensions(String svgXML) {
    String preserveAspectRatio = "preserveAspectRatio";
    String[] ecmas = svgXML.split(preserveAspectRatio);
    String header = ecmas[0];

    String[] firstLine = header.split("width");

    String newHeader = firstLine[0]+"width='" +
        SVG_WIDTH_PCT +
        "%'";
    String rest = " xmlns"+firstLine[1].split("xmlns")[1];
    String secondPart = rest.split("height")[0] + "height='" +
        SVG_HEIGHT_PCT +
        "%'";
    String body = " " +
        preserveAspectRatio +ecmas[1];

    String initial = newHeader;
    newHeader += secondPart;
    newHeader = newHeader.replaceAll("-16 -3 66 4","-16 -8 66 16");      //"-16 -3 66 4"    // "-16 -8 66 16" // -13 -2 59 3
    newHeader = newHeader.replaceAll(
        "-13 -2 59 3",
        "-16 -8 66 16");      //"-16 -3 66 4"    // "-16 -8 66 16" // -13 -2 59 3     // -13 -2 59 3

  //  if (!initial.equals(newHeader))  logger.debug("changed viewport!!!! now " + newHeader);

    //logger.debug("newHeader:\n"+newHeader);

    /**
     * <svg contentScriptType="text/ecmascript" width="2417px"
     xmlns:xlink="http://www.w3.org/1999/xlink" zoomAndPan="magnify"
     contentStyleType="text/css" viewBox="-6 -14 46 16" height="841px"
     preserveAspectRatio="xMidYMid meet" xmlns="http://www.w3.org/2000/svg"
     version="1.1">
     */return newHeader + body;
  }


  private void fixDim(String xml) {
    try {

   //   File fXmlFile = new File("/Users/mkyong/staff.xml");
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      InputSource inputSource = new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8")));

      dBuilder.setEntityResolver(new EntityResolver() {
        @Override
        public InputSource resolveEntity(String publicId, String systemId) {
          logger.warn("skipping " + publicId + " " + systemId);
          return null;
        }
      });
      Document doc = dBuilder.parse(inputSource);

      //optional, but recommended
      //read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
      doc.getDocumentElement().normalize();

      System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

      NodeList nList = doc.getElementsByTagName("staff");

      System.out.println("----------------------------");

      for (int temp = 0; temp < nList.getLength(); temp++) {

        org.w3c.dom.Node nNode = nList.item(temp);

        System.out.println("\nCurrent Element :" + nNode.getNodeName());

        if (nNode.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {

          Element eElement = (Element) nNode;
          logger.debug("elem " + eElement.getTagName() + " "+ eElement.getAttributes());

       /*   System.out.println("Staff id : " + eElement.getAttribute("id"));
          System.out.println("First Name : " + eElement.getElementsByTagName("firstname").item(0).getTextContent());
          System.out.println("Last Name : " + eElement.getElementsByTagName("lastname").item(0).getTextContent());
          System.out.println("Nick Name : " + eElement.getElementsByTagName("nickname").item(0).getTextContent());
          System.out.println("Salary : " + eElement.getElementsByTagName("salary").item(0).getTextContent());*/

        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  private int convertOneGraph(GraphModel model, DirectedGraph dag, int y, FL_PatternSearchResult result) {
    Map<String, Node> idToNode = new HashMap<String, Node>();
    final Map<Node,Double> nodeToScore = new HashMap<Node, Double>();
    List<FL_EntityMatchResult> entities = result.getEntities();

    // Order of the entities matters here...
    List<String> idsList = new ArrayList<String>(entities.size());
    for (int i=0; i<entities.size(); i++) {
    	idsList.add("");
    }
    for (FL_EntityMatchResult entity : entities) {
    	int entityInd = Integer.parseInt(entity.getUid().split("E")[1]);
    	String entityName = entity.getEntity().getUid();
    	idsList.set(entityInd, entityName);
    }
    logger.info(idsList);
    
//    List<String> ids = new ArrayList<String>();
//    for (FL_EntityMatchResult entity : entities) {
//      FL_Entity entity1 = entity.getEntity();
//
//
//      // Map<String, String> keyValue = new TreeMap<String, String>();
//      String uidValue = entity1.getUid();
//      ids.add(uidValue);
//    }
//    logger.debug("-------- result for " + ids + " = " + result.getScore());
    
    
    Set<String> ids = new TreeSet<String>();
    for (FL_EntityMatchResult entity : entities) {
      FL_Entity entity1 = entity.getEntity();


      // Map<String, String> keyValue = new TreeMap<String, String>();
      String uidValue = entity1.getUid();
      ids.add(uidValue);
    }
    logger.debug("-------- result for " + ids + " = " + result.getScore());
    
    

    
    makeGraphNodes(model, dag, idToNode, nodeToScore, entities);
    //idToNode maps node uid to node index in svg return page...

    List<Node> sources = new ArrayList<Node>();
    List<Node> targets = new ArrayList<Node>();
    addEdgesForLinks(model, dag, result, idToNode, sources, targets);
    
    sortByScore(nodeToScore, sources);
    sortByScore(nodeToScore, targets);

    int vertSpace = Y_STEP * Math.max(sources.size(), targets.size());
    int leftStep  = (int)Math.ceil(vertSpace/Math.max(1,sources.size()-1));
    int rightStep = (int)Math.ceil(vertSpace/Math.max(1,targets.size()-1));
    // logger.debug("vert " + vertSpace + " sources " + sources.size() + " targets " + targets.size() + "left " +leftStep + " right " + rightStep);
    int c = 0;
    int maxY = 0;
    for (Node source : sources) {
      float v = y + (c++ * leftStep);
      if (v > maxY) maxY = (int)v;
      source.getNodeData().setY(v);
    }

    c = 0;

    //   logger.debug("left y " + leftY + " y " +y + " vert height " + vertHeight + " step "+ystep);
    for (Node target : targets) {
      float v = y + (c++ * rightStep);
      if (v > maxY) maxY = (int)v;
      target.getNodeData().setY(v);
    }
    return maxY;
  }
  
  /*
  private int convertQueryGraph(GraphModel model, DirectedGraph dag, int y, List<String> idsList) {
	    Map<String, Node> idToNode = new HashMap<String, Node>();
	    final Map<Node,Double> nodeToScore = new HashMap<Node, Double>();
	    List<FL_EntityMatchResult> entities = result.getEntities();
	    
	    makeGraphNodes(model, dag, idToNode, nodeToScore, entities);
	    //idToNode maps node uid to node index in svg return page...

	    List<Node> sources = new ArrayList<Node>();
	    List<Node> targets = new ArrayList<Node>();
	    addEdgesForLinks(model, dag, result, idToNode, sources, targets);
	    
	    sortByScore(nodeToScore, sources);
	    sortByScore(nodeToScore, targets);

	    int vertSpace = Y_STEP * Math.max(sources.size(), targets.size());
	    int leftStep  = (int)Math.ceil(vertSpace/Math.max(1,sources.size()-1));
	    int rightStep = (int)Math.ceil(vertSpace/Math.max(1,targets.size()-1));
	    // logger.debug("vert " + vertSpace + " sources " + sources.size() + " targets " + targets.size() + "left " +leftStep + " right " + rightStep);
	    int c = 0;
	    int maxY = 0;
	    for (Node source : sources) {
	      float v = y + (c++ * leftStep);
	      if (v > maxY) maxY = (int)v;
	      source.getNodeData().setY(v);
	    }

	    c = 0;

	    //   logger.debug("left y " + leftY + " y " +y + " vert height " + vertHeight + " step "+ystep);
	    for (Node target : targets) {
	      float v = y + (c++ * rightStep);
	      if (v > maxY) maxY = (int)v;
	      target.getNodeData().setY(v);
	    }
	    return maxY;
	  }
  */

  private void makeGraphNodes(GraphModel model, DirectedGraph dag, Map<String, Node> idToNode, Map<Node, Double> nodeToScore, List<FL_EntityMatchResult> entities) {
    for (FL_EntityMatchResult entity : entities) {
      FL_Entity entity1 = entity.getEntity();


     // Map<String, String> keyValue = new TreeMap<String, String>();
      String uidValue = entity1.getUid();
      Node n0 = model.factory().newNode();//uidValue);
    //  logger.debug("uid '" + uidValue + "' : score " + entity.getScore());
      nodeToScore.put(n0,entity.getScore());
      for (FL_Property prop : entity1.getProperties()) {
       // logger.debug("got " + prop.getKey() + " : " + prop.getValue());
        if (prop.getKey().equalsIgnoreCase("name") || prop.getKey().equals("node_id")) {
          // keyValue.put(prop.getKey(),prop.getFriendlyText());
          String friendlyText = prop.getFriendlyText();
          if (friendlyText.length() > MAX_TEXT_LENGTH) friendlyText = friendlyText.substring(0, MAX_TEXT_LENGTH) + "...";
          n0.getNodeData().setLabel(friendlyText + " : " + ((int)(entity.getScore()*100)));

        }
        if (prop.getKey().equals("uid")) uidValue = ((FL_SingletonRange) prop.getRange()).getValue().toString();
      }

      if (!idToNode.containsKey(uidValue)) {
        dag.addNode(n0);
        idToNode.put(entity1.getUid(), n0);
      }
      else {
        logger.debug("skipping dup " + uidValue);
      }
    }
  }

  private void customizeApperance() {
    PreviewModel preview = Lookup.getDefault().lookup(PreviewController.class).getModel();

    preview.getProperties().putValue(PreviewProperty.DIRECTED, true);
    preview.getProperties().putValue(PreviewProperty.SHOW_NODE_LABELS, true);
    preview.getProperties().putValue(PreviewProperty.SHOW_EDGE_LABELS, true);
    preview.getProperties().putValue(PreviewProperty.EDGE_COLOR, new EdgeColor(Color.GRAY));
    preview.getProperties().putValue(PreviewProperty.NODE_LABEL_COLOR, new DependantOriginalColor(Color.BLUE));
    preview.getProperties().putValue(PreviewProperty.NODE_OPACITY, 20);
    preview.getProperties().putValue(PreviewProperty.ARROW_SIZE, 5);
    preview.getProperties().putValue(PreviewProperty.EDGE_THICKNESS, 0.1f);
    preview.getProperties().putValue(PreviewProperty.MARGIN, MARGIN);
    preview.getProperties().putValue(PreviewProperty.NODE_LABEL_FONT,
        preview.getProperties().getFontValue(PreviewProperty.NODE_LABEL_FONT).deriveFont(8));
    preview.getProperties().putValue(PreviewProperty.EDGE_LABEL_FONT,
        preview.getProperties().getFontValue(PreviewProperty.EDGE_LABEL_FONT).deriveFont(8));

   // logger.debug("dimensions : " + preview.getDimensions());

   // logger.debug("get class " + preview.getClass());

   // ((PreviewModelImpl) preview).setDimensions(new Dimension(595, 841));//1000,500));

  //  logger.debug("dimensions : " + preview.getDimensions());

  }

  private void sortByScore(final Map<Node, Double> nodeToScore, List<Node> sources) {
    Collections.sort(sources, new Comparator<Node>() {
      @Override
      public int compare(Node o1, Node o2) {
        double s1 = nodeToScore.get(o1);
        double s2 = nodeToScore.get(o2);
        return s1 < s2 ? -1 : s1 > s2 ? +1 : 0;  //To change body of implemented methods use File | Settings | File Templates.
      }
    });
  }

  private void addEdgesForLinks(GraphModel model, DirectedGraph dag,
                                FL_PatternSearchResult result,
                                Map<String, Node> idToNode, List<Node> sources, List<Node> targets) {
    int ecount = 0;
    for (FL_LinkMatchResult link : result.getLinks()) {
      FL_Link link1 = link.getLink();
      String source = link1.getSource();
      String target = link1.getTarget();
      logger.warn("link from " + source + " to " + target);

      Node snode = idToNode.get(source);
      Node tnode = idToNode.get(target);
      //     logger.debug("s " + source + " t " + target);
      if (!targets.contains(tnode)) targets.add(tnode);

      if (!sources.contains(snode)) sources.add(snode);
      Edge edge = model.factory().newEdge(/*"e" + ecount++,*/ snode, tnode, 1f, true);

      edge.getEdgeData().setLabel(getLinkProperties(link1).toString());

      dag.addEdge(edge);

    }

    setNodeXPos2(result, idToNode);
  }

  private StringBuilder getLinkProperties(FL_Link link1) {
    StringBuilder b = new StringBuilder();
    for (FL_Property prop : link1.getProperties()) {

    //  logger.debug("\tlink prop " + prop.getKey() + " " + prop.getValue());

      String s = ((FL_SingletonRange) prop.getRange()).getValue().toString();
      String toShow = "";
      try {
        Double v = Double.parseDouble(s);
        if (v > 1) {
          toShow = ""+v.intValue();
        }
        else {
          DecimalFormat f = new DecimalFormat("#.##");
          toShow = f.format(v);
        }
      } catch (NumberFormatException e) {
        toShow = s;
      }
      b.append(toShow + "/");
    }
    return b;
  }

  private void setNodeXPos(FL_PatternSearchResult result,Map<String, Node> idToNode) {
    Set<Node> currentLayer = new HashSet<Node>();

    List<FL_LinkMatchResult> links = result.getLinks();
    Set<FL_LinkMatchResult> unique = new HashSet<FL_LinkMatchResult>(links);
    populateInitialNodes(idToNode, currentLayer, unique);

    List<FL_LinkMatchResult> copy = new ArrayList<FL_LinkMatchResult>(unique);

    logger.debug("initially " + copy.size() + " links");
    for (Node n : currentLayer) logger.debug("current layer " + n.getNodeData().getLabel());

    int x = LEFT_COLUMN_X;
    int step = X_COLUMN_STEP;
    Set<Node> visited = new HashSet<Node>();
    Set<Node> currentLayer2 = new HashSet<Node>();

    while (!copy.isEmpty()) {
      for (Node n : currentLayer) {
        n.getNodeData().setX(x);
        visited.add(n);
        logger.debug("node " + n.getNodeData().getLabel() + " is at " + x);
      }
      for (Node n : currentLayer2) {
        n.getNodeData().setX(x+step);
       // visited.add(n);
        logger.debug("2 node " + n.getNodeData().getLabel() + " is at " + x);
      }
      Set<Node> nextLayer = new HashSet<Node>();
      Set<Node> nextLayer2 = new HashSet<Node>();
      List<FL_LinkMatchResult> toRemove = new ArrayList<FL_LinkMatchResult>();

      for (FL_LinkMatchResult link : copy) {
        Node snode = idToNode.get(link.getLink().getSource());
        Node tnode = idToNode.get(link.getLink().getTarget());

        if (currentLayer.contains(snode)) {
          if (visited.contains(tnode)) {
            nextLayer2.add(tnode);

          }
          else {
          nextLayer.add(tnode);
            //visited.add(tnode);
          }
          toRemove.add(link);
        }
      }

      currentLayer = nextLayer;
      currentLayer2 = nextLayer2;
      copy.removeAll(toRemove);
      //logger.debug("now " + copy.size() + " links remain");
      x += step;
    }
    if (!currentLayer.isEmpty()) {
      for (Node n : currentLayer) {
        n.getNodeData().setX(x);
        logger.debug("node " + n.getNodeData().getLabel() + " is at " + x);
      }
    }
  }

  private void setNodeXPos2(
      FL_PatternSearchResult result,
      Map<String, Node> idToNode) {
    Set<Node> currentLayer = new HashSet<Node>();

    List<FL_LinkMatchResult> links = result.getLinks();
    Set<FL_LinkMatchResult> unique = new HashSet<FL_LinkMatchResult>(links);
    populateInitialNodes(idToNode, currentLayer, unique);

    Map<Node,List<FL_Link>> nodeToLinks = new HashMap<Node, List<FL_Link>>();
    for (FL_LinkMatchResult link: unique) {
      Node snode = idToNode.get(link.getLink().getSource());

      List<FL_Link> fl_links = nodeToLinks.get(snode);
      if (fl_links == null) nodeToLinks.put(snode, fl_links = new ArrayList<FL_Link>());
      fl_links.add(link.getLink());
    }

    for (Node n : idToNode.values()) {
      n.getNodeData().setX(LEFT_COLUMN_X);
    }

    for (Node initial : currentLayer) {
      List<Node> toVisit = new ArrayList<Node>();
      toVisit.add(initial);
      int x = LEFT_COLUMN_X;

      Set<FL_Link> visited = new HashSet<FL_Link>();
      visitNext(initial, nodeToLinks, idToNode, visited, x);
     /* while (!toVisit.isEmpty()) {

      }*/
    }

    for (Node n : idToNode.values()) {
      logger.debug(n.getNodeData().getLabel() + " : " +n.getNodeData().x());
    }



  }

  private void visitNext(Node initial,  Map<Node,List<FL_Link>> nodeToLinks , Map<String, Node> idToNode,Set<FL_Link> visited, int x) {
    NodeData nodeData = initial.getNodeData();
    logger.debug("visitNext " +  nodeData.getLabel() + " x  " + x);

    if (nodeData.x() < x) {
      nodeData.setX(x);
      logger.debug("set : visitNext " + nodeData.getLabel() + " now " + x);

    }

    List<FL_Link> fl_links = nodeToLinks.get(initial);
    if (fl_links != null) {
      for (FL_Link link : fl_links) {

        Node tnode = idToNode.get(link.getTarget());


        if (x > X_COLUMN_STEP*10)
          logger.debug("stop!");
       // logger.debug("skipping link we've visited before " + link);
        else {
          visited.add(link);
          logger.debug("\tvisitNext target " + tnode.getNodeData().getLabel());
          visitNext(tnode, nodeToLinks, idToNode, visited, x + X_COLUMN_STEP);
        }
      }
    }
  }

  private void populateInitialNodes(Map<String, Node> idToNode, Set<Node> currentLayer, Set<FL_LinkMatchResult> unique) {
    for (FL_LinkMatchResult link : unique) {
      FL_Link link1 = link.getLink();
      String source = link1.getSource();
      String target = link1.getTarget();
      //logger.warn("populateInitialNodes : link from " + source + " to " + target);

      Node snode = idToNode.get(source);
      currentLayer.add(snode);
      Node tnode = idToNode.get(target);
      if (currentLayer.contains(tnode)) currentLayer.remove(tnode);
    }
  }

  private void doLayout(GraphModel model) {
    // YifanHuLayout layout = new YifanHuLayout(null, new StepDisplacement(1f));
/*     ScaleLayout layout = new ScaleLayout(null, 1);
    layout.setGraphModel(model);
    layout.resetPropertiesValues();*/


    // layout.setOptimalDistance(OPTIMAL_DISTANCE);

/*
    ForceAtlas2 layout = new ForceAtlas2(new ForceAtlas2Builder());
    layout.setGraphModel(model);
    layout.resetPropertiesValues();
    layout.setEdgeWeightInfluence(1.0);
    layout.setGravity(1.0);
    layout.setScalingRatio(2.0);
    layout.setBarnesHutTheta(1.2);
    layout.setJitterTolerance(0.1);
*/


    boolean doScale = false;
    if (doScale) {
      ScaleLayout layout = new ScaleLayout(null, 0.1);
      layout.setGraphModel(model);
      layout.resetPropertiesValues();


      layout.initAlgo();
//  for (int i = 0; i < 100 && layout.canAlgo(); i++) layout.goAlgo();
      layout.goAlgo();
    }
  }

  private void createSampleData() {
  /*
      Node n0 = model.factory().newNode("n0");
    */
/*  n0.getAttributes().setValue("x",10);
    n0.getAttributes().setValue("y",10);*//*

    n0.getNodeData().setX(10);
    n0.getNodeData().setY(10);

    for (int i = 0; i < n0.getAttributes().countValues(); i++) logger.debug("attr " + i + " : " + n0.getAttributes().getValue(0));
    NodeData nodeData = n0.getNodeData();
    nodeData.setLabel("Node 0");
   // nodeData.setLabel("<tspan dy='10'>Node 0</tspan>");
   // LayoutData layoutData = nodeData.getLayoutData();


    Node n1 = model.factory().newNode("n1");

    n1.getNodeData().setLabel("Node 1");
*/
/*    n1.getAttributes().setValue("x",20);
    n1.getAttributes().setValue("y",20);*//*


    n1.getNodeData().setX(20);
    n1.getNodeData().setY(20);

    Edge edge = model.factory().newEdge(n1, n0, 1f, true);
    edge.getEdgeData().setLabel("e1");
  //  DirectedGraph dag = model.getDirectedGraph();
  //  dag.addNode(n0);
  //  dag.addNode(n1);
    dag.addEdge(edge);
*/
  }

  private String toTable(List<Binding.ResultInfo> entities) {
	  String html = "";
	  html += "<table>";
	  // html += "<table>";
	  html += "<tr>";
	  //    html += "<th>Score</th>";
	  //    html += "<th>ID</th>";
	  List<Map<String, String>> rows = entities.get(0).rows;
	  for (String name : rows.get(0).keySet()) {
		  html += "<th>" + name + "</th>";
	  }
	  html += "</tr>";

	  for (Binding.ResultInfo ent : entities) {
		  for (Map<String, String> row : ent.rows) {
			  html += "<tr>";
			  for (String value : row.values()) {
				  html += "<td>" + value + "</td>";
			  }
			  html += "</tr>";
		  }
	  }

	  html += "</table>";
	  return html;
  }

  /**
   * @see mitll.xdata.GraphQuBEServer#getRoute(mitll.xdata.SimplePatternSearch)
   * @param entities
   * @param results
   * @return
   */
  public String toSVG(List<FL_EntityMatchDescriptor> queryEntities, FL_PatternSearchResults results, Binding binding ) {
    logger.debug("making svg for " + queryEntities.size() + " entities and " + results.getTotal() +  " results.");
    StringWriter writer = new StringWriter();

    writer.write(
        "<!DOCTYPE html>" +
        "\n" +
        "<html>\n" +
        "<head></head>\n<body>");

    //writeQueryToTable(entities, writer);
    writer.write("<h2>Results with entity scores</h2>\n");
    try {
      toSVG2(queryEntities,results, writer, binding);
    } catch (Exception e) {
      e.printStackTrace();
    }

    writer.write(" \n" +
        "</body>\n" +
        "</html>");
    //  String s = writer.toString().replaceAll("text-anchor: middle", "text-anchor: start").replaceAll("font-size=\"10\"","font-size=\"2\"");
    String s = writer.toString().replaceAll("font-size=\"10\"", "font-size=\"2\"");
         /*   s = s.replaceAll("Node 0","<tspan y='0.01'>" +"Node 0"+
              "</tspan>");*/
    return s;
  }

  /**
   * @see #toSVG(java.util.List, influent.idl.FL_PatternSearchResults, mitll.xdata.binding.Binding)
   * @param entities
   * @param writer
   */
  private void writeQueryToTable(List<Binding.ResultInfo> entities, StringWriter writer) {
	  Map<String,List<Binding.ResultInfo>> typeToEntities = new HashMap<String, List<Binding.ResultInfo>>();
	  logger.debug("query entities.size() = " + entities.size());
	  for ( Binding.ResultInfo entity : entities) {
		  logger.debug("\tentity = " + entity);
		  List<Binding.ResultInfo> resultInfos = typeToEntities.get(entity.getTable());
		  if (resultInfos == null) typeToEntities.put (entity.getTable(), resultInfos = new ArrayList<Binding.ResultInfo>());
		  resultInfos.add(entity);
	  }
	  for (Map.Entry<String, List<Binding.ResultInfo>> entities2 : typeToEntities.entrySet()) {
		  List<Binding.ResultInfo> value = entities2.getValue();
		  writer.write("<h3>" + entities2.getKey() + "</h3>\n");
		  writer.write(toTable(value));
	  }
  }
}
