package com.test.hbase;


import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;


public class HbaseMain {
	
	public static void main(String[] args) {
		
		HbaseAPI hbase = null;
		Connection connection = null;
		Admin admin = null;
		try {
			hbase = new HbaseAPI();
			connection = hbase.getConnection();
			admin = hbase.getAdmin();
			
//			-------------------DDL----------------------
		
			//判断fruit表是否存在
			boolean tableExist = hbase.isTableExist("wugong");
			//建立武功秘籍表
//			hbase.createTable("aaa:wugong2", "quanfa");
			//添加列族
//			hbase.addColumnFamily("gongfu", "xinfa");
			//命名空间下的表
//			hbase.listTableNamesByNamespace("test");
			//删除表
//			hbase.dropTable("gongfu");
		
//			-------------------DML----------------------
			
//			hbase.putOneColumnData("wugong1", "001", "quanfa", "name", "taiJiQuan");
//			ArrayList<String[]> list = new ArrayList<String[]>();
//			String[] str1 = {"quanfa","b","c"};
//			String[] str2 = {"quanfa","b1","c1"};
//			String[] str3 = {"quanfa","c1","c1"};
//			list.add(str1);
//			list.add(str2);
//			list.add(str3);
//			hbase.putData("wugong", "004", list);
//			删除
//			hbase.deleteByRowkey("wugong", "001");
//			hbase.deleteByColumn("wugong", "003", "quanfa", "b");
//			hbase.deleteByColumn("wugong", "004", "quanfa", "c1");
//			hbase.deleteByColumnFamily("wugong", "004", "quanfa");
			//扫描表数据
			hbase.scanData("wugong");
			
			
			
			
			
			
			
			
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			hbase.closeConnectionAndAdmin(connection, admin);
		}
		

		
	}

}
