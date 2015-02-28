import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
public class SSSP {

	public static TitanGraph create()
	{
		Configuration conf=new BaseConfiguration();
		conf.setProperty("storage.backend","hbase");
		conf.setProperty("storage.hostname", "localhost");
		//conf.setProperty("","");
		TitanGraph g=TitanFactory.open(conf);
	//	g.createKeyIndex("age", Vertex.class);
		return g;
		
	}
	public static TitanGraph load(TitanGraph g,String tableName) throws Exception
	{
		org.apache.hadoop.conf.Configuration config= HBaseConfiguration.create();
		HTable table =new HTable(config,tableName);
		Scan s =new Scan();
		ResultScanner ss =table.getScanner(s);
		
	//	Get get =new Get(("A").getBytes());
//		Result reult=table.get(get);
		//System.out.print(result);
		HashMap<String,String> edge=new HashMap<String,String>();
		for(Result r=ss.next();r!=null;r=ss.next())
		{
			byte[] NoOfRoads=r.getValue(Bytes.toBytes("Village"), Bytes.toBytes("NoOfRoads"));
			byte[] LengthOfRoads=r.getValue(Bytes.toBytes("Village"), Bytes.toBytes("LengthOfRoads"));
			byte[] Neighbors=r.getValue(Bytes.toBytes("Village"), Bytes.toBytes("Neighbors"));
			Vertex v=g.addVertex(null);
			v.setProperty("Name",new String(r.getRow()));
			v.setProperty("NoOfRoads", new String(NoOfRoads));
			edge.put(new String(r.getRow()), new String(LengthOfRoads)+"_"+new String(Neighbors));
		}
		
		Iterator itr=edge.entrySet().iterator();
		while(itr.hasNext())
		{
		
			Map.Entry<String,String> entry=(Entry<String, String>) itr.next();
			String Node=entry.getKey().toString();
			String EdgeInfo[]=entry.getValue().split("_");
			String []EdgeValue=EdgeInfo[0].split(",");
			String []TargetVertex=EdgeInfo[1].split(",");
			Vertex v1=g.getVertices("Name", Node).iterator().next();
			for(int i=0;i<EdgeValue.length;i++)
			{
				Vertex v2=g.getVertices("Name", TargetVertex[i]).iterator().next();
				
				Edge e=g.addEdge(null, v1, v2, "Connect");
				e.setProperty("Length",EdgeValue[i]);
			}
			
		}
		
		
		
		return g;
	}
	
	private static void printGraph(TitanGraph g)
	{
		System.out.println("Node Name:\tNo of Roads:");
		for(Vertex vertex:g.getVertices())
		{
			System.out.print(vertex.getProperty("Name")+"\t");
			System.out.print(vertex.getProperty("NoOfRoads")+"\t");
			System.out.println();
		}
		System.out.println("Edge:\tEdge Cost:");
		for(Edge edge:g.getEdges())
		{
			System.out.print(edge.getVertex(Direction.IN).getProperty("Name")+"==>"+edge.getVertex(Direction.OUT).getProperty("Name"));
			System.out.print("\t"+edge.getProperty("Length"));
			System.out.println();
		}
		System.out.println();
	}
	
	private static TitanGraph FindSSSP(TitanGraph g, String SSSP_Source) {
		// TODO Auto-generated method stub
		//The logic is inspired by Dijkshtra's Algorithm
		
		boolean loop=true;
		
		
		//Step:1  Initialize distance of every non-root vertex with Infinite and Put them in Unvisited Set 
		for(Vertex v:g.getVertices())
		{
			v.setProperty("Distance", Integer.MAX_VALUE);
			v.setProperty("Status", "Unvisited");
		}
		
		//Step:2 Initialize distance of root vertex with zero.
		Vertex Source_Vertex=g.getVertices("Name", SSSP_Source).iterator().next();
		Source_Vertex.setProperty("Distance", 0);
		
		//Step:3 loop over unvisited vertex set
		while(loop)
		{
			
			//Step:4 select vertex from unvisited set with shortest distance and call it current
			int Min_Dist=Integer.MAX_VALUE;
			String Min_Val=null;
			for(Vertex v:g.getVertices("Status","Unvisited"))
			{
				if(Integer.parseInt(v.getProperty("Distance").toString()) <= Min_Dist)
				{
					Min_Dist=Integer.parseInt(v.getProperty("Distance").toString());
					Min_Val=v.getProperty("Name").toString();
				}
			}
			Vertex Current=g.getVertices("Name",Min_Val).iterator().next();
			Current.setProperty("Status","Current");
			
			
			//Step:5 get all the neighbor unvisited node and if(neighbor distance>(current node distance+edge cost)) than set their distance=(current node distance+edge cost)  
			int Curr_Dis=Integer.parseInt(Current.getProperty("Distance").toString());
			int Dest_Dis = 0;
			int Edge_Value=0;
			for(Vertex Visiting_Node:Current.getVertices(Direction.OUT,"Connect"))
			{
				if(!(Visiting_Node.getProperty("Status").toString().equals("Visited")))
				{
					Dest_Dis=Integer.parseInt(Visiting_Node.getProperty("Distance").toString());
					for(Edge e1:Current.query().direction(Direction.OUT).labels("Connect").edges())
					{
					
						if(Visiting_Node.getProperty("Name").toString().equals(e1.getVertex(Direction.IN).getProperty("Name").toString()))
						{
							Edge_Value=Integer.parseInt(e1.getProperty("Length").toString());
						}
					}
					if(Dest_Dis > Edge_Value+Curr_Dis)
					{
						Visiting_Node.setProperty("Distance",(Edge_Value+Curr_Dis));
					}		
				}
			}
			
			//Step:6 remove current node from unvisited node
			Current.setProperty("Status", "Visited");
			
			
			//Step:7 continue loop untill all the nodes are visited 
			if(!(g.getVertices("Status","Unvisited").iterator().hasNext()))
				loop=false;
			
			
		}
		System.out.println("======SSSP=======");
		System.out.println("Node:\tMinimum Distance:");
		for(Vertex v:g.getVertices())
		{
			
			System.out.println(v.getProperty("Name").toString()+"\t"+v.getProperty("Distance").toString());
		}
		return g;
	}
	private static void writeInHBase(TitanGraph SSSP_Graph, String TableName) throws Exception
	{
		// TODO Auto-generated method stub
		org.apache.hadoop.conf.Configuration config = null;
		
		config = HBaseConfiguration.create();
		config.clear();
		String family="NodeInfo";
		HBaseAdmin admin=new HBaseAdmin(config);
		if(admin.tableExists(TableName))
		{
			System.out.println("Table Still Exist.");
		}
		else
		{
			HTableDescriptor tableDesc=new HTableDescriptor(TableName);
			tableDesc.addFamily(new HColumnDescriptor(family));
			admin.createTable(tableDesc);
			System.out.println("create Table:\t"+TableName);
		}
		
		HTable table =new HTable(config,TableName);
		for(Vertex v:SSSP_Graph.getVertices())
		{
			Put put=new Put(Bytes.toBytes(v.getProperty("Name").toString()));
			put.add(Bytes.toBytes(family),Bytes.toBytes("Distance"),Bytes.toBytes(v.getProperty("Distance").toString()));
			table.put(put);
		}
		
		table.flushCommits();
		table.close();
		
	}
	public static void main(String[] args) throws Exception
	{
		// TODO Auto-generated method stub

		TitanGraph g=(TitanGraph) SSSP.create();
		g.createKeyIndex("Name",Vertex.class);
		g.createKeyIndex("Status",Vertex.class);
		g=(TitanGraph) SSSP.load(g,"MapOfVillage");
		
		System.out.println("======Initial Graph=======");
		SSSP.printGraph(g);
		
		String SSSP_Source="A";
		TitanGraph SSSP_Graph=(TitanGraph) SSSP.FindSSSP(g,SSSP_Source);
		
		String TableName="SSSP_Village";
		SSSP.writeInHBase(SSSP_Graph,TableName);
	}
	
}