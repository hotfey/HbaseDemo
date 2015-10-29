package com.secoo.hbase.definition;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Definition {
	public static Configuration configuration;

	static {
		configuration = HBaseConfiguration.create();
	}

	public void createTable(String tableName, String[] families)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		for (String family : families) {
			hTableDescriptor.addFamily(new HColumnDescriptor(family));
		}
		hBaseAdmin.createTable(hTableDescriptor);
		hBaseAdmin.close();
	}

	public void deleteTable(String tableName)
			throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
		hBaseAdmin.disableTable(tableName);
		hBaseAdmin.deleteTable(tableName);
		hBaseAdmin.close();
	}
}
