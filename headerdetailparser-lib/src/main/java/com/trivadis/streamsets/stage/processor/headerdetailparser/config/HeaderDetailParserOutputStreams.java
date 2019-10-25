package com.trivadis.streamsets.stage.processor.headerdetailparser.config;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum HeaderDetailParserOutputStreams implements Label {
	  HEADER("Header"),
//	  DETAIL("Detail"),
	  HEADERDETAIL("Header&Detail");

	  private final String label;

	  HeaderDetailParserOutputStreams(String label) {
	    this.label = label;
	  }

	  @Override
	  public String getLabel() {
	    return label;
	  }
}