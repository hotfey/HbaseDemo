package com.secoo.hbase.filter;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

import com.secoo.hbase.filter.Manipulation;

public class ManipulationTest {
	@Ignore
	@Test
	public void testRowPut() {
		String tableName = "";
		String family = "";
		String[] qualifiers = { "", "" };
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.rowPut(tableName, family, qualifiers);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testRowDelete() {
		String tableName = "";
		String[] rowKeys = { "" };
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
		String tableName = "";
		String[] rowKeys = { "" };
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
		String tableName = "";
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
		String tableName = "";
		String family = "";
		String qualifier = "";
		String value = "";
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
		String tableName = "";
		String[] families = { "", "" };
		String[] qualifiers = { "", "" };
		String[] values = { "", "" };
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
		String tableName = "";
		String family = "";
		String qualifier = "";
		String value = "";
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.filterListRegexScan(tableName, family, qualifier, value);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
