/**
 * Copyright 2014 MIT Lincoln Laboratory, Massachusetts Institute of Technology
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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import mitll.xdata.dataset.bitcoin.binding.BitcoinBinding;
import mitll.xdata.db.DBConnection;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;


/**
 * Collapses SortedEdgeListConstructor and SPDAndTopologyAndSPathIndexConstructor2 classes into one
 * 
 * @author Charlie Dagli (dagli@ll.mit.edu)
 * MIT Lincoln Laboratory
 *
 * extending code from @author Manish Gupta (gupta58@illinois.edu)
 */
public class MultipleIndexConstructor {
	
	private static Logger logger = Logger.getLogger(MultipleIndexConstructor.class);

	public static String baseDir = "data/bitcoin/graphs";
	public static String outDir = "data/bitcoin/indices";
	public static String graphFile = "";
	public static String typesFile = "";
	public static int D=1;
	
	public static HashMap<Integer, Integer> node2Type = new HashMap<Integer, Integer>();
	public static HashMap<String, ArrayList<Edge>> sortedEdgeLists = new HashMap<String, ArrayList<Edge>>();
	public static HashMap<Integer,ArrayList<String>> ordering = new HashMap<Integer, ArrayList<String>>();
	public static HashMap<Integer, HashSet<ArrayList<Integer>>> paths = new HashMap<Integer, HashSet<ArrayList<Integer>>>();
	public static DecimalFormat twoDForm = new DecimalFormat("#.####");
	public static Graph g = new Graph();
	
	public static int totalTypes = 0;
	

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Throwable {

		// Set up a simple configuration that logs on the console.
		BasicConfigurator.configure();

		// grab arguments		
		baseDir = args[0];
		outDir = args[1];
		graphFile = args[2];
		typesFile = args[3];
		D = Integer.parseInt(args[4]);

		long time1=new Date().getTime();

		/**
		 * Functionality of SortedEdgeListConstructor
		 */
		logger.info("Building Sorted Edge List Index...");

		//load the graph
		g.loadGraph(new File(baseDir,graphFile));

		//load types file
		loadTypesFile();		

		// Create Typed Edges
		createTypedEdges();

		// Load and Sort Edges from Graph
		//loadAndSortEdges();
		populateSortedEdgeLists();

		//save the sorted edge lists
		saveSortedEdgeList();

		//test method that computes totalTypes
		totalTypes = 0;
		computeTotalTypes();
		logger.debug("Computed number of types: "+totalTypes);	

		/**
		 * Functionality of SPDAndTopologyAndSPathIndexConstructor
		 */
		//hash map for all possible "edge-type" paths: i.e. doubles,triples,...D-tuples
		//this gets you the "official" ordering
		logger.info("Computing Edge-Type Path Ordering...");
		computeEdgeTypePathOrdering();


		logger.info("Computing SPD, Topology and SPath Indices...");

		computeIndices();	

		long time2=new Date().getTime();
		logger.info("Time:"+(time2-time1));

	}
	
	

	/**
	 * Compute SPD, Topology and SPath Indices 
	 * 
	 * @throws IOException
	 */
	public static void computeIndices() throws IOException {
		
		//Make outDir if neccessary
		File directory = new File(outDir);
		if (!directory.exists() && !directory.mkdirs()) 
			throw new IOException("Could not create directory: " + outDir);
		
		//setup output files
//		String filename=graphFile.split("\\.")[0]+"."+Integer.toString(D)+".spath";
//		logger.info(graphFile);
//		logger.info(filename);
//		BufferedWriter out = new BufferedWriter(new FileWriter(new File(outDir, filename)));
//		String topologyFilename=graphFile.split("\\.")[0]+"."+Integer.toString(D)+".topology";
//		BufferedWriter outTopology = new BufferedWriter(new FileWriter(new File(outDir, topologyFilename)));
//		String spdFilename=graphFile.split("\\.")[0]+"."+Integer.toString(D)+".spd";
//		BufferedWriter outSPD = new BufferedWriter(new FileWriter(new File(outDir, spdFilename)));
		
		String filename=BitcoinBinding.DATASET_ID+"."+Integer.toString(D)+".spath";
		BufferedWriter out = new BufferedWriter(new FileWriter(new File(outDir, filename)));
		String topologyFilename=BitcoinBinding.DATASET_ID+"."+Integer.toString(D)+".topology";
		BufferedWriter outTopology = new BufferedWriter(new FileWriter(new File(outDir, topologyFilename)));
		String spdFilename=BitcoinBinding.DATASET_ID+"."+Integer.toString(D)+".spd";
		BufferedWriter outSPD = new BufferedWriter(new FileWriter(new File(outDir, spdFilename)));
		
		//for(int i=1;i<=g.numNodes;i++)
		for(int i=0;i<g.numNodes;i++)
		{
			if(i%100==0)
				//System.err.println("Nodes processed: "+i+" out of "+g.numNodes);
				logger.debug("Nodes processed: "+i+" out of "+g.numNodes);
			out.write(i+"\t");
			outTopology.write(i+"\t");
			outSPD.write(i+"\t");
			//int n=g.node2NodeIdMap.get(i);//this is a big-bug fixed...
			int n=i;//internalID
			HashSet<Integer> queue = new HashSet<Integer>();
			HashMap<Integer, Double> sumWeight = new HashMap<Integer, Double>();
			queue.add(n);
			sumWeight.put(n,0.);
			HashSet<Integer> considered = new HashSet<Integer>();
			considered.add(n);
			paths = new HashMap<Integer, HashSet<ArrayList<Integer>>>();
			ArrayList<Integer> ll  = new ArrayList<Integer>();
			ll.add(n);
			HashSet<ArrayList<Integer>> hs = new HashSet<ArrayList<Integer>>();
			hs.add(ll);
			paths.put(n, hs);
			for(int d=0;d<D;d++)
			{
				//perform BFS from each node.
				HashMap<Integer, HashSet<ArrayList<Integer>>> newPaths = new HashMap<Integer, HashSet<ArrayList<Integer>>>();
				HashSet<Integer> newQueue = new HashSet<Integer>();
				HashMap<Integer, Double> newSumWeight = new HashMap<Integer, Double>();
				for(int q:queue)
				{
					ArrayList<Edge> nbrs= g.inLinks.get(q); 
					for(Edge e:nbrs)
					{
						int qDash=e.src;
						double newWt=sumWeight.get(q)+e.weight;
						if((newSumWeight.containsKey(qDash)&&newSumWeight.get(qDash)<newWt)||(!newSumWeight.containsKey(qDash)&&!considered.contains(qDash)))
						{
							considered.add(qDash);
							newQueue.add(qDash);
							newSumWeight.put(qDash, sumWeight.get(q)+e.weight);
						}
						if(newSumWeight.containsKey(qDash)||(!newSumWeight.containsKey(qDash)&&!considered.contains(qDash)))
						{
							HashSet<ArrayList<Integer>> hsai = new HashSet<ArrayList<Integer>>();
							if(newPaths.containsKey(qDash))
								hsai=newPaths.get(qDash);
							for(ArrayList<Integer> ai:paths.get(q))
							{
								ArrayList<Integer> nali = new ArrayList<Integer>(ai);
								nali.add(qDash);
								hsai.add(nali);
							}
							newPaths.put(qDash, hsai);
						}
					}
				}
				queue= newQueue;
				sumWeight=newSumWeight;
				paths= newPaths;
				HashMap<Integer, ArrayList<Integer>> map = new HashMap<Integer, ArrayList<Integer>>();
				for(int q:queue)
				{
					int actualID=g.nodeId2NodeMap.get(q);
					int label=node2Type.get(actualID);
					ArrayList<Integer> al = new ArrayList<Integer>();
					if(map.containsKey(label))
						al= map.get(label);
					al.add(actualID);
					map.put(label, al);
				}
				
				//processing for SPath index.
				for(Integer s:map.keySet())
					Collections.sort(map.get(s));
				for(int s=1;s<=totalTypes;s++)
				{
					if(map.containsKey(s))
					{
						out.write(map.get(s).size()+"#");
						for(int t:map.get(s))
							out.write(t+",");
					}
					else
					{
						out.write("0#,");
					}
					out.write(";");
				}
				out.write(" ");
				
				//process pathsq
				HashMap<String, ArrayList<Integer>> topo = new HashMap<String, ArrayList<Integer>>();
				HashMap<String, Double> spd = new HashMap<String, Double>();
				for(int ii:paths.keySet())
				{
					for(ArrayList<Integer> p:paths.get(ii))
					{
						String types="";
						double totWeight=0;
						for(int j=0;j<p.size();j++)
						{
							int a=p.get(j);
							if(j!=0)
							{
								int aDash=p.get(j-1);
								totWeight+=g.getEdge(a, aDash).weight;
								int b=g.nodeId2NodeMap.get(a);
								types+=node2Type.get(b);
							}
						}
						if((spd.containsKey(types)&&spd.get(types)<totWeight)||!spd.containsKey(types))
							spd.put(types, totWeight);
						int lastNode=ii;
						ArrayList<Integer> l = new ArrayList<Integer>();
						if(topo.containsKey(types))
							l=topo.get(types);
						if(!l.contains(lastNode))
							l.add(lastNode);
						topo.put(types, l);
					}
				}
				
				//write out topology index.
				for(String o:ordering.get(d+1))
				{
					if(!topo.containsKey(o))
						outTopology.write("0;");
					else
					{
						int t = topo.get(o).size();
						outTopology.write(t+";");
					}
				}
				//outTopology.write(" ");
				
				//write out spd index.
				for(String o:ordering.get(d+1))
				{
					if(!spd.containsKey(o))
						outSPD.write("0.;");
					else
					{
						double t = spd.get(o);
						outSPD.write(Double.valueOf(twoDForm.format(t))+";");
					}
				}
				//outSPD.write(" ");
			}
			out.write("\n");
			outTopology.write("\n");
			outSPD.write("\n");
		}
		out.close();
		outTopology.close();
		outSPD.close();
	}

	/**
	 * Compute all EdgeType Paths of length d, put in hash map
	 */
	public static void computeEdgeTypePathOrdering() {
		for(int d=1;d<=D;d++)
			ordering.put(d, new ArrayList<String>());
		for(int i=1;i<=totalTypes;i++)
			ordering.get(1).add(i+"");
		for(int d=2;d<=D;d++)
		{
			for(int i=1;i<=totalTypes;i++)
			{
				for(String s:ordering.get(d-1))
				{
					if(s.length()==d-1)
						ordering.get(d).add(s+i);
				}
			}
		}
	}
		
	/**
	 * Figure out how many types there are (in the case where we're not loading everything from file)
	 * 
	 */
	public static void computeTotalTypes() {

		for(int key:node2Type.keySet()) {
			int type = node2Type.get(key);
			if(type>totalTypes)
				totalTypes=type;			
		}
	}

	
	/**
	 * Save out sorted edge list
	 * 
	 * @param graphFile
	 * @throws IOException
	 */
	public static void saveSortedEdgeList() throws IOException {
		
		//Make outDir if neccessary
		File directory = new File(outDir);
		if (!directory.exists() && !directory.mkdirs()) 
			throw new IOException("Could not create directory: " + outDir);
		
		//Save out....
		for(String key:sortedEdgeLists.keySet())
		{
			//String fileName=graphFile.split("\\.")[0]+"_"+key+".list";
			String fileName=BitcoinBinding.DATASET_ID+"_"+key+".list";
			
			BufferedWriter out = new BufferedWriter(new FileWriter(new File(outDir, fileName)));
			ArrayList<Edge> arr = sortedEdgeLists.get(key);
			for(Edge e:arr)
				out.write(e.src+"#"+e.dst+"#"+e.weight+"\n");
			out.close();
		}
	}

	/**
	 * Load and sort edges from Graph transactions file
	 * 
	 * @param graphFile
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void loadAndSortEdges()
			throws FileNotFoundException, IOException {
		
		//load edges
		BufferedReader in = new BufferedReader(new FileReader(new File(baseDir, graphFile)));
		String str = "";
		while((str=in.readLine())!=null)
		{
			if(str.startsWith("#"))
				continue;
			String tokens[]=str.split("#");
			int from=Integer.parseInt(tokens[0]);
			int to=Integer.parseInt(tokens[1]);
			if(from>to)
				continue;
			double wt=Double.parseDouble(tokens[2]);
			int fromType=node2Type.get(from);
			int toType = node2Type.get(to);
			if(fromType>toType)
			{
				int tmp=fromType;
				fromType=toType;
				toType=tmp;
				tmp=from;
				from=to;
				to=tmp;
			}
			Edge e = new Edge(from, to, wt);
			ArrayList<Edge> arr = sortedEdgeLists.get(fromType+"#"+toType);
			arr.add(e);
			sortedEdgeLists.put(fromType+"#"+toType, arr);
		}
		in.close();
		
		//sort the arraylists in descending order
		for(String key:sortedEdgeLists.keySet())
			Collections.sort(sortedEdgeLists.get(key), new EdgeComparator());
	}

	/**
	 * Sister method to loadAndSortEdges.
	 * --Does the same thing, just from the graph file loaded in 
	 * --as a IndexConstruction.Graph data structure
	 * 
	 * @param None (except it expects class variables g and node2Type have been populated)
	 */
	public static void populateSortedEdgeLists() {
		
		for (Edge e : g.edges) {
			//get internal node-ids for 
			int from = g.nodeId2NodeMap.get(e.src);
			int to = g.nodeId2NodeMap.get(e.dst);
			double weight = e.weight;
			//logger.info("from index is: "+from);
			//logger.info("to index is: "+to);
			if (from>to)
				continue;
			
			int fromType=node2Type.get(from);
			int toType=node2Type.get(to);
			if(fromType>toType)
			{
				int tmp=fromType;
				fromType=toType;
				toType=tmp;
				tmp=from;
				from=to;
				to=tmp;
			}
			ArrayList<Edge> arr = sortedEdgeLists.get(fromType+"#"+toType);
			arr.add(new Edge(from, to, weight));
			sortedEdgeLists.put(fromType+"#"+toType, arr);
		}
		
		//sort the arraylists in descending order
		for(String key:sortedEdgeLists.keySet())
			Collections.sort(sortedEdgeLists.get(key), new EdgeComparator());				
	}
	
	/**
	 * Create "typed edges" i.e. node of type i is connected to node of type j
	 * 
	 * @param totalTypes
	 */
	public static void createTypedEdges() {

		//create typed edges
		for(int t=1;t<=totalTypes;t++)
		{
			for(int t2=t;t2<=totalTypes;t2++)
			{
				String key=t+"#"+t2;
				sortedEdgeLists.put(key, new ArrayList<Edge>());
			}
		}
	}
	
	/**
	 * Load types file
	 * 
	 * @param typesFile
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void loadTypesFile()
			throws FileNotFoundException, IOException {
		
		//load types file
		BufferedReader in = new BufferedReader(new FileReader(new File(baseDir,typesFile)));
		String str = "";
		while ((str = in.readLine()) != null) {
			String tokens[] = str.split("\\t");
			int node=Integer.parseInt(tokens[0]);
			int type=Integer.parseInt(tokens[1]);
			node2Type.put(node, type);
			if(type>totalTypes)
				totalTypes=type;
		}
		in.close();
	}
	
	/**
	 * Load types info from database
	 * 
	 * @param dbConnection
	 * @return
	 * @throws Exception
   * @see mitll.xdata.dataset.bitcoin.ingest.BitcoinIngestSubGraph#computeIndices(String, DBConnection)
	 */
	public static void loadTypesFromDatabase(DBConnection dbConnection, String tableName, String uidColumn, String typeColumn)
			throws Exception {

    logger.info("loadTypesFromDatabase " + tableName + " " + uidColumn + " " + typeColumn);

		/*
		 * Do query
		 */
		Connection connection = dbConnection.getConnection();

		String sqlQuery = "select "+uidColumn+", "+typeColumn+" from "+tableName+";";

		logger.info("sql " + sqlQuery + " on " + connection);

		PreparedStatement queryStatement = connection.prepareStatement(sqlQuery);
		ResultSet rs = queryStatement.executeQuery();

		/*
		 * Loop-through result set, populate node2Type
		 */
		int c=0;
		while (rs.next()) {
			c++;
			if (c % 100000 == 0) {logger.debug("read  " +c);}

			//Retrieve by column name
			int guid  = rs.getInt(uidColumn);
			int type = rs.getInt(typeColumn);

			//logger.info("UID: "+guid+"\tTYPE: "+type);
			node2Type.put(guid, type);
			if(type>totalTypes)
				totalTypes=type;   
		}

		rs.close();
		queryStatement.close();
		//connection.close();		
	}
	
	/**
	 * Setter for pre-loaded node2Type HashMap
	 */
	public static void setNode2Type(HashMap<Integer, Integer> in) {
		node2Type = in;
	}
	
	/**
	 * Setter for pre-loaded graph g
	 */
	public static void setGraph(Graph in) {
		g = in;
	}

	/**
	 * Getter for graph g
	 */
	public static Graph getGraph() {
		return g;
	}	
}
