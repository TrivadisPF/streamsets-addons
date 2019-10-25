package com.trivadis.streamsets.stage.processor.headerdetailparser;

import com.streamsets.pipeline.api.ConfigDef;

public class HeaderExtractorConfig {

	  @ConfigDef(
		      required = true,
		      type = ConfigDef.Type.NUMBER,
		      defaultValue = "",
		      label = "Line Number",
		      description="The line number from where the value should be extracted from. First line starts from 1.",
		      displayPosition = 10,
		      min = 1,
		      max = 9999,
		      group = "PARSER"
      )
	  public Integer lineNumber;
	
	  @ConfigDef(
		      required = false,
		      type = ConfigDef.Type.STRING,
		      defaultValue = "",
		      label = "Key",
		      description="A fixed value for the key. Will overwrite the value extracted from group 1 of regular expression",
		      displayPosition = 20,
		      group = "PARSER"
		  )
	  public String key;
	  
		
	  @ConfigDef(
		      required = true,
		      type = ConfigDef.Type.STRING,
		      defaultValue = "",
		      label = "Regular Expression",
		      description="A regular expression which extracts key as group 1 and value as group 2",
		      displayPosition = 30,	      
		      group = "PARSER"
		  )
 
	  public String regex;
}
