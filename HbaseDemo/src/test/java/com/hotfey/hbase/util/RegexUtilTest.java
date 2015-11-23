package com.hotfey.hbase.util;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Ignore;
import org.junit.Test;

public class RegexUtilTest {
	@Ignore
	@Test
	public void range() {
		int prefixLength = 4;
		long startNumeric = 1448017934380L;
		long stopNumeric = 1448017934580L;
		startNumeric = 100000001L;
		stopNumeric = 100000110L;

		Map<String, String> map = RegexUtil.regexNumericRange(prefixLength, startNumeric, stopNumeric);
		if (map.isEmpty()) {
			System.out.println("regex empty!");
		} else {
			String input = "";
			int ok = 0;
			int no = 0;
			for (Long i = startNumeric; i <= stopNumeric; i++) {
				input = randomNumeric(prefixLength) + i.toString();
				Pattern pattern = Pattern.compile(map.get("startRegex") + "&&" + map.get("stopRegex"));
				Matcher matcher = pattern.matcher(input);
				if (matcher.find()) {
					ok++;
				} else {
					no++;
				}
			}
			System.out.println("all:" + (stopNumeric - startNumeric + 1));
			System.out.println("ok:" + ok);
			System.out.println("no:" + no);
		}
	}

	private String randomNumeric(int prefixLength) {
		int[] ints = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < prefixLength; i++) {
			stringBuffer.append(ints[(int) (Math.random() * 10)]);
		}
		return stringBuffer.toString();
	}
}
