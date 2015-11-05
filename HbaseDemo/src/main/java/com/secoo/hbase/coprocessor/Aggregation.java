package com.secoo.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

public class Aggregation {
	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
	}

	public long rowCount(String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.rowCount(hTable, new LongColumnInterpreter(), scan);
	}

	public double sum(String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.sum(hTable, new DoubleColumnInterpreter(), scan);
	}

	public double avg(String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);

		int qualifierCount = 0;
		for (int i = 0; i < ProtobufUtil.toScan(scan).getColumnCount(); i++) {
			qualifierCount += ProtobufUtil.toScan(scan).getColumn(i).getQualifierCount();
		}
		return aggregationClient.avg(hTable, new DoubleColumnInterpreter(), scan) * qualifierCount;
	}

	public double max(String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.max(hTable, new DoubleColumnInterpreter(), scan);
	}

	public double min(String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.min(hTable, new DoubleColumnInterpreter(), scan);
	}

}
