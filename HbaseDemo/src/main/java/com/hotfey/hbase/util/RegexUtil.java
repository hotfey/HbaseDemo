package com.hotfey.hbase.util;

import java.util.HashMap;
import java.util.Map;

public class RegexUtil {
	public static Map<String, String> regexNumericRange(int prefixLength, long startNumeric, long stopNumeric) {
		return regexNumericRange(prefixLength, startNumeric, stopNumeric, 0);
	}

	public static Map<String, String> regexNumericRange(int prefixLength, long startNumeric, long stopNumeric,
			int suffixLength) {
		Map<String, String> map = new HashMap<>();
		
		String startIndex = String.valueOf(startNumeric);
		String stopIndex = String.valueOf(stopNumeric);
		int length = (startIndex.length() == stopIndex.length() ? startIndex.length() : 0);

		StringBuffer startStringBuffer = new StringBuffer();
		startStringBuffer.append("^");
		startStringBuffer.append("\\d{");
		startStringBuffer.append(prefixLength);
		startStringBuffer.append("}");
		startStringBuffer.append(regexString(startIndex, stopIndex, length).get("startRegex"));
		startStringBuffer.append("\\d{");
		startStringBuffer.append(suffixLength);
		startStringBuffer.append("}");
		
		StringBuffer stopStringBuffer = new StringBuffer();
		stopStringBuffer.append("^");
		stopStringBuffer.append("\\d{");
		stopStringBuffer.append(prefixLength);
		stopStringBuffer.append("}");
		stopStringBuffer.append(regexString(startIndex, stopIndex, length).get("stopRegex"));
		stopStringBuffer.append("\\d{");
		stopStringBuffer.append(suffixLength);
		stopStringBuffer.append("}");
		
		map.put("startRegex", startStringBuffer.toString());
		map.put("stopRegex", stopStringBuffer.toString());

		return map;
	}

	private static Map<String, String> regexString(String startIndex, String stopIndex, int length) {
		if (length == 0) {
			return null;
		}
		if (startIndex.compareTo(stopIndex) > 0) {
			return null;
		}

		String baseString = "";
		String startIndexVariable = "";
		String stopIndexVariable = "";

		int i = 0;
		for (i = 0; i <= length; i++) {
			baseString = startIndex.substring(0, length - i);
			if (stopIndex.startsWith(baseString)) {
				startIndexVariable = startIndex.substring(length - i);
				stopIndexVariable = stopIndex.substring(length - i);
				break;
			}
		}

		if (i == 0) {
			Map<String, String> map = new HashMap<>();
			map.put("startRegex", baseString);
			map.put("stopRegex", baseString);
			return map;
		} else {
			return createRegex(baseString, startIndexVariable, stopIndexVariable, i);
		}
	}

	private static Map<String, String> createRegex(String baseString, String startIndexVariable,
			String stopIndexVariable, int length) {
		Map<String, String> map = new HashMap<>();

		char[] startChar = startIndexVariable.toCharArray();
		char[] stopChar = stopIndexVariable.toCharArray();
		StringBuffer stringBuffer = new StringBuffer();
		stringBuffer.append(baseString);

		stringBuffer.append("[");
		stringBuffer.append(startChar[0]);
		stringBuffer.append("-");
		stringBuffer.append(stopChar[0]);
		stringBuffer.append("]");

		StringBuffer startStringBuffer = new StringBuffer();

		for (int i = 0; i < length - 1; i++) {
			startStringBuffer.append("\\d(?<!");
			for (int j = 0; j <= i; j++) {
				startStringBuffer.append(startChar[j]);
			}
			int temp = Integer.parseInt(String.valueOf(startChar[i + 1]));
			if (temp == 0) {
				startStringBuffer.append("[^0-");
				startStringBuffer.append("9])");
			} else {
				startStringBuffer.append("[0-");
				startStringBuffer.append(temp - 1);
				startStringBuffer.append("])");
			}
		}

		StringBuffer stopStringBuffer = new StringBuffer();

		for (int i = 0; i < length - 1; i++) {
			stopStringBuffer.append("\\d(?<!");
			for (int j = 0; j <= i; j++) {
				stopStringBuffer.append(stopChar[j]);
			}
			int temp = Integer.parseInt(String.valueOf(stopChar[i + 1]));
			if (temp == 9) {
				stopStringBuffer.append("[^0-");
				stopStringBuffer.append("9])");
			} else {
				stopStringBuffer.append("[");
				stopStringBuffer.append(temp + 1);
				stopStringBuffer.append("-9])");
			}
		}

		map.put("startRegex", stringBuffer.toString() + startStringBuffer.toString());
		map.put("stopRegex", stringBuffer.toString() + stopStringBuffer.toString());

		return map;
	}
}
