package com.trivadis.streamsets.stage.processor.headerdetailparser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TestRegex {

	Pattern p = Pattern.compile("(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)");

	@Test
	public void testWithPosition() {
		String input = "Position,ROW01";
		Matcher matcher = p.matcher(input);
		System.out.println(matcher.find());
		System.out.println(matcher.group(1));
		System.out.println(matcher.group(2));
	}
	
	@Test
	public void testWithSerial() {
		String input = "Device serial,01B0B628";
		Matcher matcher = p.matcher(input);
		System.out.println(matcher.find());
		System.out.println(matcher.group(1));
		System.out.println(matcher.group(2));
	}
	
	@Test
	public void testWithTStart() {
		String input = "TStart:, 110212";
		Matcher matcher = p.matcher(input);
		System.out.println(matcher.find());
		System.out.println(matcher.group(1));
		System.out.println(matcher.group(2));
	}

	
	@Test
	public void test2() {
		String input = "-----/r";
		Pattern p = Pattern.compile("^-----");
		Matcher matcher = p.matcher(input);
		System.out.println(matcher.find());
	}
	
	
}
