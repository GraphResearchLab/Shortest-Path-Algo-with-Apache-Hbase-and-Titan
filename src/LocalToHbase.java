
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class LocalToHbase {
	
	private static Configuration config = null;
	
	static
	{
		config = HBaseConfiguration.create();
		config.clear();
	}
	
	public static void createTable(String tableName,String[] familys) throws Exception
	{
		HBaseAdmin admin=new HBaseAdmin(config);
		if(admin.tableExists(tableName))
		{
			System.out.println("Table Still Exist");
		}
		else
		{
			HTableDescriptor tableDesc=new HTableDescriptor(tableName);
			for(int i=0;i<familys.length;i++)
			{
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create Table"+tableName);
			
		}
	}
	
	public static void addRecord(String tableName,String rowKey,String family,String[] qualifier,String[] value) throws Exception
	{
		HTable table =new HTable(config,tableName);
		Put put=new Put(Bytes.toBytes(rowKey));
		for(int i=1;i<value.length;i++)
			put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier[i]),Bytes.toBytes(value[i]));
		table.put(put);
		table.flushCommits();
		table.close();
		System.out.println("Insert record:"+rowKey+" to Table:"+tableName);
		
	}
	
	
	public static void main(String args[]) throws Exception
	{
		String tableName="MapOfVillage";
		String[] familys={"Village"};
		String[] quanti={"VillageName","NoOfRoads","LengthOfRoads","Neighbors"};
		LocalToHbase.createTable(tableName, familys);
		
		String inputline="";
		File inputFile=new File(args[0]);
		FileReader filereader=new FileReader(inputFile);
		BufferedReader filebuffer=new BufferedReader(filereader);
		
		
		while((inputline=filebuffer.readLine())!=null)
		{
			String values[]=inputline.split("\\t");
			
			LocalToHbase.addRecord(tableName,values[0], familys[0],quanti,values);
//			LocalToHbase.addRecord(tableName,values[0], familys[0],"NoOfRoads",values[1]);
//			LocalToHbase.addRecord(tableName,values[0], familys[0],"LengthOfRoads",values[2]);
//			LocalToHbase.addRecord(tableName,values[0], familys[0],"Neighbors",values[3]);
		}
		
		filebuffer.close();
		filereader.close();
		
		
	}

}