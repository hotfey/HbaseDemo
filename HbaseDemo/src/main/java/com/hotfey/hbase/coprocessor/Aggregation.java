package com.hotfey.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;

public class Aggregation {
	public long rowCount(Configuration configuration, String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.rowCount(hTable, new LongColumnInterpreter(), scan);
	}

	public double sum(Configuration configuration, String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.sum(hTable, new DoubleColumnInterpreter(), scan);
	}

	public double avg(Configuration configuration, String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.avg(hTable, new DoubleColumnInterpreter(), scan);
	}

	public double max(Configuration configuration, String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.max(hTable, new DoubleColumnInterpreter(), scan);
	}

	public double min(Configuration configuration, String tableName, Scan scan) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.min(hTable, new DoubleColumnInterpreter(), scan);
	}

}
