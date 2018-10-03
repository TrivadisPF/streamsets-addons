package com.trivadis.streamsets.stage.processor.headerdetailparser;

import java.util.ArrayList;

public class StringSplitter {

	public static String[] fastSplit(String string, char delimiter) {
		return fastSplit(string, delimiter, 0);
	}
	
	public static String[] fastSplit(String string, char delimiter, int limit) {
	    /*
	     *  fastpath of String.split()
	     *  
	     *  [NOTE]
	     *  it will remove empty token in the end
	     *  it will not remove in-between empty tokens
	     *  the same behavior as String.split(String regex)
	     *  
	     *  [EXAMPLE]
	     *  string = "boo\tboo\tboo\t\t\tboo\t\t\t\t\t";
	     *  strings = fastSplit(string, '\t') -> [boo, boo, boo, , , boo]
	     */
	    int off = 0;
	    int next = 0;
	    ArrayList<String> list = new ArrayList<>();
	    while ((next = string.indexOf(delimiter, off)) != -1) {
	        list.add(string.substring(off, next));
	        off = next + 1;

	        // break if limit is reached (one below limit, last one will be set below)
	        if (limit != 0 && list.size() == (limit-1)) {
	    		break;
	    	}
	    }
	    // If no match was found, return this
	    if (off == 0)
	        return new String[] { string };

	    // Add remaining segment
	    list.add(string.substring(off, string.length()));

	    // Construct result
	    int resultSize = list.size();
	    while (resultSize > 0 && list.get(resultSize - 1).length() == 0)
	        resultSize--;
	    String[] result = new String[resultSize];
	    return list.subList(0, resultSize).toArray(result);
	}

	public static String[] fastSplit(String string, String delimiter) {
		return fastSplit(string, delimiter, 0);
	}

	public static String[] fastSplit(String string, String delimiter, int limit) {
	    /*
	     *  fastpath of String.split()
	     *  
	     *  [NOTE]
	     *  it will remove empty token in the end
	     *  it will not remove in-between empty tokens
	     *  the same behavior as String.split(String regex)
	     *  
	     *  [EXAMPLE]
	     *  string = "boo\tboo\tboo\t\t\tboo\t\t\t\t\t";
	     *  strings = fastSplit(string, '\t') -> [boo, boo, boo, , , boo]
	     */
	    int off = 0;
	    int next = 0;
	    ArrayList<String> list = new ArrayList<>();
	    while ((next = string.indexOf(delimiter, off)) != -1) {
	    	list.add(string.substring(off, next));
	    	off = next + delimiter.length();
	    	
	        // break if limit is reached (one below limit, last one will be set below)
		    if (limit != 0 && list.size() == (limit-1)) {
	    		break;
	    	}
	    }
	    // If no match was found, return this
	    if (off == 0)
	        return new String[] { string };

	    // Add remaining segment
	    list.add(string.substring(off, string.length()));

	    // Construct result
	    int resultSize = list.size();
	    while (resultSize > 0 && list.get(resultSize - 1).length() == 0)
	        resultSize--;
	    String[] result = new String[resultSize];
	    return list.subList(0, resultSize).toArray(result);
	}
	
}
