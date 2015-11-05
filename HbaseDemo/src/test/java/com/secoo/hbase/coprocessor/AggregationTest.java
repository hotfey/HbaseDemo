package com.secoo.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

public class AggregationTest {
	@Ignore
	@Test
	public void testRowCount() {
		String tableName = "";
		String family = "";

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
		String tableName = "";
		String family = "";
		String qualifier = "";
		String filterQualifier = "";
		String filterQualifierValue = "";

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(filterQualifier));

		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(filterQualifier), CompareOp.EQUAL,
				Bytes.toBytes(filterQualifierValue));

		scan.setFilter(filter);

		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("sum:%s", aggregation.sum(tableName, scan) * ProtobufUtil.toScan(scan).getColumnCount());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testAvg() {
		String tableName = "";
		String family = "";
		String qualifier = "";
		String filterQualifier = "";
		String filterQualifierValue = "";

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(filterQualifier));

		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(filterQualifier), CompareOp.EQUAL,
				Bytes.toBytes(filterQualifierValue));

		scan.setFilter(filter);

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
		String tableName = "";
		String family = "";
		String qualifier = "";

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
		String tableName = "";
		String family = "";
		String qualifier = "";

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
