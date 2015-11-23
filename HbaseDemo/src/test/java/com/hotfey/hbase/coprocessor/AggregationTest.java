package com.hotfey.hbase.coprocessor;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;
import org.junit.Test;

import com.hotfey.hbase.util.RegexUtil;

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

		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(filterQualifier),
				CompareOp.EQUAL, Bytes.toBytes(filterQualifierValue));

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
	public void testAvgs() {
		String tableName = "";
		String family = "";
		String qualifier = "";
		String filterQualifier = "";
		String filterQualifierValue;

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(filterQualifier));

		FilterList filterList = new FilterList(Operator.MUST_PASS_ONE);

		Filter filter;
		int sum = 0;
		int count = 999;
		for (int i = 1; i <= count; i++) {
			sum += i;
			filterQualifierValue = i + "";
			filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(filterQualifier), CompareOp.EQUAL,
					Bytes.toBytes(filterQualifierValue));
			filterList.addFilter(filter);
		}

		scan.setFilter(filterList);
		scan.setLoadColumnFamiliesOnDemand(true);

		Aggregation aggregation = new Aggregation();
		try {
			System.out.printf("avg:%s \n", aggregation.avg(tableName, scan));
			System.out.println("sum:" + sum);
			System.out.println("count:" + count);
			System.out.println("sum/count:" + sum * 1.0 / count * 1.0);
			System.out.println("sum/(count+1):" + sum * 1.0 / (count + 1) * 1.0);
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
		String startTime = "";
		String endTime = "";

		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(filterQualifier));

		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);

		int prefixLength = 3;
		long startNumeric = 0;
		long stopNumeric = 0;

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		Date date = null;
		try {
			date = simpleDateFormat.parse(startTime);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		startNumeric = date.getTime();

		try {
			date = simpleDateFormat.parse(endTime);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		stopNumeric = date.getTime();

		Map<String, String> map = RegexUtil.regexNumericRange(prefixLength, startNumeric, stopNumeric);

		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(map.get("startRegex")));
		filterList.addFilter(filter);
		filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(map.get("stopRegex")));
		filterList.addFilter(filter);

		filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(filterQualifier), CompareOp.EQUAL,
				Bytes.toBytes(filterQualifierValue));
		filterList.addFilter(filter);

		scan.setFilter(filterList);

		Aggregation aggregation = new Aggregation();
		try {
			long startTimesMillis = System.currentTimeMillis();
			for (int i = 0; i < 1; i++) {
				System.out.printf("avg:%s\n", aggregation.avg(tableName, scan));
			}
			long endTimeMillis = System.currentTimeMillis();
			System.out.printf("use time:%s\n", endTimeMillis - startTimesMillis);
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
