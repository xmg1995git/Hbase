package com.test.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


import org.apache.hadoop.classification.InterfaceAudience.Public;


public class APP {

	public static void main(String[] args) {
//		String[] strings1 = {"aa","bb","cc"};
//		String[] strings2 = {"11","22","33"};
//		List<String[]> list = new ArrayList<String[]>();
//	
//		list.add(strings1);
//		list.add(strings2);
//		
//		if(list.size() == 2) {
//			for(String[] strings : list) {
//				System.out.println("---"+strings[0]+strings[1]+strings[2]);
//				System.out.println("---");
//			}
//		}
//		System.out.println(list.size());
//		List list1 = new ArrayList(3);
//		list1.add("dss");
//		System.out.println(list1);
		System.out.println(null == "");
		String aaa = "";
		aa("");

	}
	public static void aa(String ...a) {
		String[] aaa = {""};
		System.out.println(aaa == a);
		System.out.println(a.length);
		
	}
}
