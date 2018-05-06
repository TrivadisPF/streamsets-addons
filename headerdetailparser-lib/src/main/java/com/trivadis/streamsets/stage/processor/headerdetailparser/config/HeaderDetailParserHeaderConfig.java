package com.trivadis.streamsets.stage.processor.headerdetailparser.config;

import java.util.List;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;
import com.trivadis.streamsets.stage.processor.headerdetailparser.HeaderExtractorConfig;

public class HeaderDetailParserHeaderConfig {
	@ListBeanModel
	@ConfigDef(
		      required = false,
		      type = ConfigDef.Type.MODEL,
		      defaultValue = "",
		      label = "Header Extractors",
		      description="A regular expression which extracts key as group 1 and value as group 2",
		      displayPosition = 40,
		      group = "HEADER"
		  )
	public List<HeaderExtractorConfig> headerExtractorConfigs;
	@ConfigDef(
		      required = false,
		      type = ConfigDef.Type.STRING,
		      defaultValue = "",
		      label = "Header/Detail Separator",
		      description="A regular expression which identifiey a given line as a spearator line between the headers and the detail lines",
		      displayPosition = 45,
		      group = "HEADER"
		  )
	public String headerDetailSeparator;
	@ConfigDef(
		      required = false,
		      type = ConfigDef.Type.NUMBER,
		      defaultValue = "",
		      label = "Number of Header Lines",
		      description="The number of header lines to remove",
		      displayPosition = 50,
		      group = "HEADER"
		  )
	public Integer nofHeaderLines;

}