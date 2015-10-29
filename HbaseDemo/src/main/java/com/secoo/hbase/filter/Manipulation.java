package com.secoo.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class Manipulation {
	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
	}

	public void rowPut(String tableName, String family, String qualifier) throws IOException {
		HTable hTable = new HTable(configuration, tableName);
		for (int i = 0; i < 100; i++) {
			String value = i + "";
			Put put = new Put(("row" + i).getBytes());
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(Double.parseDouble(value)));
			hTable.put(put);
			System.out.printf("row put family:%s, qualifier:%s, value:%s \n", family, qualifier, value);
		}
		hTable.close();
	}

	public void rowDelete(String tableName, String[] rowKeys) throws IOException {
		HTable hTable = new HTable(configuration, tableName);
		List<Delete> list = new ArrayList<Delete>();
		for (String rowKey : rowKeys) {
			Delete delete = new Delete(Bytes.toBytes(rowKey));
			list.add(delete);
			System.out.printf("delete rowKey:%s \n", rowKey);
		}
		hTable.delete(list);
		System.out.printf("delete rowKeys success!");
		hTable.close();
	}

	public void rowGet(String tableName, String[] rowKeys) throws IOException {
		HTable hTable = new HTable(configuration, tableName);
		for (String rowKey : rowKeys) {
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = hTable.get(get);
			for (Cell cell : result.rawCells()) {
				System.out.printf("row:%s, family:%s, qualifier:%s, value:%s \n",
						Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		hTable.close();
	}

	public void allScan(String tableName) throws IOException {
		HTable hTable = new HTable(configuration, tableName);
		Scan scan = new Scan();
		ResultScanner resultScanner = hTable.getScanner(scan);
		for (Result result : resultScanner) {
			for (Cell cell : result.rawCells()) {
				System.out.printf("row:%s, family:%s, qualifier:%s, value:%s \n",
						Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toDouble(CellUtil.cloneValue(cell)));
			}
		}
		hTable.close();
	}

	public void filterScan(String tableName, String family, String qualifier, String value) throws IOException {
		HTable hTable = new HTable(configuration, tableName);
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(qualifier), CompareOp.EQUAL,
				Bytes.toBytes(value));
		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = hTable.getScanner(scan);
		for (Result result : resultScanner) {
			for (Cell cell : result.rawCells()) {
				System.out.printf("row:%s, family:%s, qualifier:%s, value:%s \n",
						Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		hTable.close();
	}

	public void filterListScan(String tableName, String[] families, String[] qualifiers, String[] values)
			throws IOException {
		HTable hTable = new HTable(configuration, tableName);
		Filter filter = null;
		List<Filter> liFilters = new ArrayList<Filter>();
		filter = new SingleColumnValueFilter(Bytes.toBytes(families[0]), Bytes.toBytes(qualifiers[0]), CompareOp.EQUAL,
				Bytes.toBytes(values[0]));
		liFilters.add(filter);
		filter = new SingleColumnValueFilter(Bytes.toBytes(families[1]), Bytes.toBytes(qualifiers[1]), CompareOp.EQUAL,
				Bytes.toBytes(values[1]));
		liFilters.add(filter);

		FilterList filterList = new FilterList(liFilters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		ResultScanner resultScanner = hTable.getScanner(scan);
		for (Result result : resultScanner) {
			for (Cell cell : result.rawCells()) {
				System.out.printf("row:%s, family:%s, qualifier:%s, value:%s \n",
						Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		hTable.close();
	}

	public void filterListCompareScan(String tableName, String[] families, String[] qualifiers, String[] values)
			throws IOException {
		HTable hTable = new HTable(configuration, tableName);
		Filter filter = null;
		List<Filter> liFilters = new ArrayList<Filter>();
		filter = new SingleColumnValueFilter(Bytes.toBytes(families[0]), Bytes.toBytes(qualifiers[0]), CompareOp.EQUAL,
				Bytes.toBytes(values[0]));
		liFilters.add(filter);
		filter = new SingleColumnValueFilter(Bytes.toBytes(families[1]), Bytes.toBytes(qualifiers[1]),
				CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(values[1]));
		liFilters.add(filter);
		filter = new SingleColumnValueFilter(Bytes.toBytes(families[1]), Bytes.toBytes(qualifiers[1]),
				CompareOp.LESS_OR_EQUAL, Bytes.toBytes("65"));
		liFilters.add(filter);

		FilterList filterList = new FilterList(liFilters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		ResultScanner resultScanner = hTable.getScanner(scan);
		for (Result result : resultScanner) {
			for (Cell cell : result.rawCells()) {
				System.out.printf("row:%s, family:%s, qualifier:%s, value:%s \n",
						Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		hTable.close();
	}

	public void filterListRegexScan(String tableName, String[] families, String[] qualifiers, String[] values)
			throws IOException {
		HTable hTable = new HTable(configuration, tableName);
		Filter filter = null;
		List<Filter> liFilters = new ArrayList<Filter>();
		filter = new SingleColumnValueFilter(Bytes.toBytes(families[0]), Bytes.toBytes(qualifiers[0]), CompareOp.EQUAL,
				Bytes.toBytes(values[0]));
		liFilters.add(filter);
		filter = new SingleColumnValueFilter(Bytes.toBytes(families[1]), Bytes.toBytes(qualifiers[1]), CompareOp.EQUAL,
				new RegexStringComparator(values[1] + "$"));
		liFilters.add(filter);

		FilterList filterList = new FilterList(liFilters);
		Scan scan = new Scan();
		scan.setFilter(filterList);
		ResultScanner resultScanner = hTable.getScanner(scan);
		for (Result result : resultScanner) {
			for (Cell cell : result.rawCells()) {
				System.out.printf("row:%s, family:%s, qualifier:%s, value:%s \n",
						Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		hTable.close();
	}
}
