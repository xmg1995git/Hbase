package com.test.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.tools.javac.util.List;
import com.sun.xml.internal.xsom.impl.scd.Iterators.Map;



public class HbaseAPI {
	/**
	 * 获取configuration对象，并连接connection
	 * DDL:
	 * 判断表是否存在
	 * 创建表
	 * 创建命名空间
	 * 删除表
	 * DML:
	 * 增删改查
	 * get scan
	 */
	Configuration conf = null;
	Connection conn = null;
	Admin admin = null;
	
	private static final String HOSTNAME = "hadoop112,hadoop113,hadoop114";
	private static final String ZKPORT = "2181";
	
	/**
	 * 创建命名空间
	 * @param namespace
	 * @throws IOException
	 */
	public void createNameSpace(String namespace) throws IOException {
		try {
			//创建命名空间描述器
			NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("").build();
			//创建命名空间
			admin.createNamespace(namespaceDescriptor);
		} catch (NamespaceExistException e) {
			System.out.println(namespace+"--命名空间已存在！");
		}
	}
	/**
	 * 命名空间下的表
	 * @param nameSpace
	 * @throws IOException 
	 */
	public void listTableNamesByNamespace(String nameSpace) throws IOException {
		TableName[] tableNames = admin.listTableNamesByNamespace(nameSpace);
		for(TableName tableName : tableNames) {
			System.out.println(tableName);
		}
	}
	
	/**
	 * 创建表
	 * @param tableName
	 * @param cfs
	 */
	public void createTable(String tableName,String... cfs) {
		if(cfs.length <= 0) {
			System.out.println("请设置列族信息！！！");
			return;
		}
		if(isTableExist(tableName)) {
			System.out.println(tableName+"--表已存在！无法创建！！！");
			return;
		}
		try {
			//表描述器构造器
			TableDescriptorBuilder tdb = 
					TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
			//列描述器
			for(String cf:cfs) {
				ColumnFamilyDescriptor cfd = 
						ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build();
				tdb.setColumnFamily(cfd);
			}
			admin.createTable(tdb.build());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 删除表
	 * @param tableName
	 */
	public void dropTable(String tableName) {
		if(isTableExist(tableName)) {
			try {
				admin.disableTable(TableName.valueOf(tableName));
				admin.deleteTable(TableName.valueOf(tableName));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else {
			System.out.println(tableName+"--表不存在！");
		}
	}
	
	/**
	 * 表添加列族
	 * @param tableName
	 * @param cfs
	 * @throws IOException 
	 */
	public void addColumnFamily(String tableName,String cf) throws IOException {
		try {
			//表描述器构造器
			TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
			//列描述器
			ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build();
			tdb.setColumnFamily(cfd);
			admin.addColumnFamily(TableName.valueOf(tableName), cfd);
		} catch (InvalidFamilyOperationException e) {
			System.out.println(cf+"列族已存在！！！");
		}
	}
	
	/**
	 * 判断tableName表是否存在
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	public boolean isTableExist(String tableName) {
		boolean exists = false;
		try {
			exists = admin.tableExists(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
//		System.out.println(exists ? tableName+"--表存在" : tableName+"--表不存在");
		return exists;
	}
	
	/**
	 * 获取连接
	 * HOSTNAME = "hadoop112,hadoop113,hadoop114";
	 * ZKPORT = "2181";
	 * @return
	 * @throws IOException
	 */
	public Connection getConnection() throws IOException {
		conf = getConfiguration(HOSTNAME,ZKPORT);
		conn = ConnectionFactory.createConnection(conf);
		return conn;
	}
	/**
	 * 获取连接
	 * @param hostname
	 * @param zkPort
	 * @return
	 * @throws IOException
	 */
	public Connection getConnection(String hostname,String zkPort) throws IOException {
		conf = getConfiguration(hostname,zkPort);
		conn = ConnectionFactory.createConnection(conf);
		return conn;
	}
	
	/**
	 * 关闭conn连接
	 * @param conn
	 */
	public void closeConnectionAndAdmin(Connection conn,Admin admin) {
		try {
				if(conn != null) {
					conn.close();	
				}
				if(admin != null) {
					admin.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
	
	/**
	 * admin对象
	 * @return
	 * @throws IOException
	 */
	public Admin getAdmin() throws IOException {
		admin = conn.getAdmin();
		return admin;
	}
	
	/**
	 * 配置信息
	 * @param hostname
	 * @param zkPort
	 * @return
	 */
	private Configuration getConfiguration(String hostname,String zkPort) {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hostname);
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		return conf;
	}
	
//	---------------------DML---------------------------
	/**
	 * 向表中插入一列数据
	 * @param tableName
	 * @param rowkey
	 * @param cf
	 * @param cn
	 * @param value
	 * @throws IOException 
	 */
	public void putOneColumnData(String tableName,String rowKey,String cf,String cn,String value) throws IOException {
		Table table = conn.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
		table.put(put);
		table.close();
	}
	
	/**
	 * 向表中插入数据
	 * List<String[]>，String[]依次是是列族-列名-列值
	 * @param tableName
	 * @param rowKey
	 * @param map
	 * @throws IOException 
	 */
	public void putData(String tableName,String rowKey,ArrayList<String[]> datas) throws IOException {
		Table table = conn.getTable(TableName.valueOf(tableName));
		Put put = new Put(Bytes.toBytes(rowKey));
		for(String[] data : datas) {
			put.addColumn(Bytes.toBytes(data[0]), Bytes.toBytes(data[1]), Bytes.toBytes(data[2]));
			table.put(put);	
		}
		table.close();
	}
	
	/**
	 * 删除一行
	 * @param tableName
	 * @param rowkey
	 * @throws IOException 
	 */
	public void deleteByRowkey(String tableName,String rowkey) throws IOException {
		deleteData(tableName, rowkey, "");
	}
	
	/**
	 * 删除某一行列族
	 * @param tableName
	 * @param rowkey
	 * @param cf
	 * @throws IOException
	 */
	public void deleteByColumnFamily(String tableName,String rowkey,String cf) throws IOException {
		deleteData(tableName, rowkey, cf);
	}
	
	/**
	 * 删除某列
	 * @param tableName
	 * @param rowkey
	 * @param cf
	 * @param columns
	 * @throws IOException
	 */
	public void deleteByColumn(String tableName,String rowkey,String cf,String ...columns) throws IOException {
		deleteData(tableName, rowkey, cf, columns);
	}
	
	private void deleteData(String tableName,String rowkey,String cf,String ...columns) throws IOException {
		Table table = conn.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		//删除一行内容
		if(cf == "" && columns.length == 0) {
			table.delete(delete);
			table.close();
			return;
		}
		//删除某一行的列族内容
		if(cf != "" && columns.length == 0) {
			delete.addFamily(Bytes.toBytes(cf));
			table.delete(delete);
			table.close();
			return;
		}
		//删除某列内容
		if(cf != "" && columns.length != 0) {
			for(String column : columns) {
				delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column));
				table.delete(delete);	
			}
			table.close();
			return;
		}
	}
	
	/**
	 * scan 获取表数据
	 * @param tableName
	 * @throws IOException
	 */
	public void scanData(String tableName) throws IOException {
		Table table = conn.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			for(Cell cell : result.rawCells()) {
				System.out.println(
						"ROWKEY:"+Bytes.toString(CellUtil.cloneRow(cell))+
						"CF:"+Bytes.toString(CellUtil.cloneFamily(cell))+
						"CN:"+Bytes.toString(CellUtil.cloneQualifier(cell))+
						"VALUE:"+Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		table.close();
	}
	

	
	

}
