package com.secoo.hbase.filter;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import com.secoo.hbase.filter.Manipulation;

public class ManipulationTest {
	@Ignore
	@Test
	public void testRowPut() {
		String tableName = "t1";
		String family = "f1";
		String qualifier = "q1";
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.rowPut(tableName, family, qualifier);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testRowDelete() {
		String tableName = "java";
		String[] rowKeys = { "row1" };
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.rowDelete(tableName, rowKeys);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testRowGet() {
		String tableName = "java";
		String[] rowKeys = { "row1" };
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.rowGet(tableName, rowKeys);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testAllScan() {
		String tableName = "t1";
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.allScan(tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testFilterScan() {
		String tableName = "java";
		String family = "cf1";
		String qualifier = "name";
		String value = "name1";
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.filterScan(tableName, family, qualifier, value);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testFilterListScan() {
		String tableName = "java";
		String[] families = { "cf1", "cf2" };
		String[] qualifiers = { "name", "age" };
		String[] values = { "name1", "55" };
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.filterListScan(tableName, families, qualifiers, values);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testFilterListRegexScan() {
		String tableName = "java";
		String[] families = { "cf1", "cf2" };
		String[] qualifiers = { "name", "age" };
		String[] values = { "name1", "5" };
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.filterListRegexScan(tableName, families, qualifiers, values);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
