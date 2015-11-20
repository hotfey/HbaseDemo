package com.hotfey.hbase.definition;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.Ignore;
import org.junit.Test;

import com.hotfey.hbase.definition.Definition;

public class DefinitionTest {
	@Ignore
	@Test
	public void testCreateTable() {
		String tableName = "";
		String[] families = { "" };
		Definition definition = new Definition();
		try {
			definition.createTable(tableName, families);
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
	public void testDeleteTAble() {
		String tableName = "";
		Definition definition = new Definition();
		try {
			definition.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.printf("table %s delete success!", tableName);
	}
}
