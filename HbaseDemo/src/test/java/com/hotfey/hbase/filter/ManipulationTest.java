package com.hotfey.hbase.filter;

import java.io.IOException;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import com.hotfey.hbase.util.RegexUtil;

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
	public void testRowFilterScan() {
		String tableName = "";

		int prefixLength = 3;
		long startNumeric = 1448017934380L;
		long stopNumeric = 1448017934580L;
		startNumeric = 100000001L;
		stopNumeric = 100000111L;
		Map<String, String> map = RegexUtil.regexNumericRange(prefixLength, startNumeric, stopNumeric);

		Manipulation manipulation = new Manipulation();
		try {
			manipulation.rowFilterScan(tableName, map);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(map.get("startRegex"));
		System.out.println(map.get("stopRegex"));
	}

	@Ignore
	@Test
	public void testColumnFilterScan() {
		String tableName = "";
		String family = "";
		String qualifier = "";
		String value = "";
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.columnFilterScan(tableName, family, qualifier, value);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testColumnFilterListScan() {
		String tableName = "";
		String[] families = { "", "" };
		String[] qualifiers = { "", "" };
		String[] values = { "", "" };
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.columnFilterListScan(tableName, families, qualifiers, values);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testColumnFilterListRegexScan() {
		String tableName = "";
		String family = "";
		String qualifier = "";
		String value = "";
		Manipulation manipulation = new Manipulation();
		try {
			manipulation.columnFilterListRegexScan(tableName, family, qualifier, value);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
