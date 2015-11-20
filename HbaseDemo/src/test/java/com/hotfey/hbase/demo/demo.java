package com.hotfey.hbase.demo;

import org.junit.Ignore;
import org.junit.Test;

public class demo {
	@Ignore
	@Test
	public void stringReverse(){
		for(int i = 0; i < 10; i ++){
			System.out.println(new StringBuffer(10000 + i + "").reverse().toString());
		}
	}
}
