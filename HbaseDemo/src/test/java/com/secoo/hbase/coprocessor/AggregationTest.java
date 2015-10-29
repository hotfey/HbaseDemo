package com.secoo.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.Ignore;
import org.junit.Test;

public class AggregationTest {
	@Ignore
	@Test
	public void testCreateTableCoprocessor() {
		String tableName = "t1";
		String family = "f1";
		Aggregation aggregation = new Aggregation();
		try {
			aggregation.createTableCoprocessor(tableName, family);
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
		String tableName = "t1";
		Aggregation aggregation = new Aggregation();
		try {
			aggregation.addCoprocessor(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.printf("Add table %s coprocessor success!", tableName);
	}

	@Ignore
	@Test
	public void testRowCount() {
		String tableName = "t1";
		String family = "f1";
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("rowCount:%s", aggregation.rowCount(tableName, family));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testSum() {
		String tableName = "t1";
		String family = "f1";
		String qualifier = "q1";
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("sum:%s", aggregation.sum(tableName, family, qualifier));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testAvg() {
		String tableName = "t1";
		String family = "f1";
		String qualifier = "q1";
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("avg:%s", aggregation.avg(tableName, family, qualifier));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testMax() {
		String tableName = "t1";
		String family = "f1";
		String qualifier = "q1";
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("max:%s", aggregation.max(tableName, family, qualifier));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testMin() {
		String tableName = "t1";
		String family = "f1";
		String qualifier = "q1";
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("min:%s", aggregation.min(tableName, family, qualifier));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
