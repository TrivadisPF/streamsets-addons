package com.trivadis.streamsets.stage.processor.headerdetailparser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TestRegex {

	@Test
	public void test() {
		String input = "Position,ROW01";
		Pattern p = Pattern.compile("(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)");
		Matcher matcher = p.matcher(input);
		System.out.println(matcher.find());
		System.out.println(matcher.group(1));
		System.out.println(matcher.group(2));
	}

}
