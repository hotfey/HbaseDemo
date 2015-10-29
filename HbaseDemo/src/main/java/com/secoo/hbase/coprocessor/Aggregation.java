package com.secoo.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class Aggregation {
	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
	}

	public void createTableCoprocessor(String tableName, String family)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		hTableDescriptor.addFamily(new HColumnDescriptor(family));
		String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
		hTableDescriptor.addCoprocessor(coprocessorClassName);
		hBaseAdmin.createTable(hTableDescriptor);
		hBaseAdmin.close();
	}

	public void addCoprocessor(String tableName)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
		hBaseAdmin.disableTable(tableName);
		HTableDescriptor hTableDescriptor = hBaseAdmin.getTableDescriptor(Bytes.toBytes(tableName));
		hTableDescriptor.addCoprocessor(coprocessorClassName);
		hBaseAdmin.modifyTable(tableName, hTableDescriptor);
		hBaseAdmin.enableTable(tableName);
		hBaseAdmin.close();
	}

	public long rowCount(String tableName, String family) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(family));
		scan.setFilter(new FirstKeyOnlyFilter());

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.rowCount(hTable, new LongColumnInterpreter(), scan);
	}

	public double sum(String tableName, String family, String qualifier) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.sum(hTable, new DoubleColumnInterpreter(), scan);
	}

	public double avg(String tableName, String family, String qualifier) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.avg(hTable, new DoubleColumnInterpreter(), scan);
	}

	public double max(String tableName, String family, String qualifier) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.max(hTable, new DoubleColumnInterpreter(), scan);
	}

	public double min(String tableName, String family, String qualifier) throws IOException, Throwable {
		HTable hTable = new HTable(configuration, tableName);
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

		AggregationClient aggregationClient = new AggregationClient(configuration);
		return aggregationClient.min(hTable, new DoubleColumnInterpreter(), scan);
	}

}
