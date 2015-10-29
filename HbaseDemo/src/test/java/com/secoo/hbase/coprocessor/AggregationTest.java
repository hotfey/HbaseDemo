package com.secoo.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

public class AggregationTest {
	@Ignore
	@Test
	public void testRowCount() {
		String tableName = "t1";
		String family = "f1";
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(family));
		scan.setFilter(new FirstKeyOnlyFilter());
		
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("rowCount:%s", aggregation.rowCount(tableName, scan));
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
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("sum:%s", aggregation.sum(tableName, scan));
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
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("avg:%s", aggregation.avg(tableName, scan));
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
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("max:%s", aggregation.max(tableName, scan));
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
		
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		
		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("min:%s", aggregation.min(tableName, scan));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
