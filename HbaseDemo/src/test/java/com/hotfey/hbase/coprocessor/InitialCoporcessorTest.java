package com.hotfey.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.Ignore;
import org.junit.Test;

import com.hotfey.hbase.coprocessor.InitialCoprocessor;

public class InitialCoporcessorTest {
	@Ignore
	@Test
	public void testCreateTableCoprocessor() {
		String tableName = "";
		String family = "";
		InitialCoprocessor initialCoprocessor = new InitialCoprocessor();
		try {
			initialCoprocessor.createTableCoprocessor(tableName, family);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.printf("create table %s success!", tableName);
	}

	@Ignore
	@Test
	public void testAddCoprocessor() {
		String tableName = "";
		InitialCoprocessor InitialCoprocessor = new InitialCoprocessor();
		try {
			InitialCoprocessor.addCoprocessor(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.printf("Add table %s coprocessor success!", tableName);
	}
}
