package com.hotfey.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class InitialCoprocessor {
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

}
