package com.trivadis.streamsets.stage.processor.headerdetailparser.config;

import java.util.List;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

public class HeaderDetailParserDetailsConfig {
	@ValueChooserModel(HeaderChooserValue.class)
	@ConfigDef(
		      required = true,
		      type = ConfigDef.Type.MODEL,
		      defaultValue = "NO_HEADER",
		      label = "Column Header Line",
		      description="Is there a Column Header line in front of the detail lines and should it be used.",
		      displayPosition = 10,
		      group = "DETAILS"
		  )
	public DetailsColumnHeaderType detailsColumnHeaderType;
	
	@ConfigDef(
	      required = true,
	      type = ConfigDef.Type.STRING,
	      defaultValue = ",",
	      label = "Separator",
	      description = "Regular expression to use for splitting the field. If trying to split on a RegEx meta" +
	          " character \".$|()[{^?*+\\\", the character must be escaped with \\",
	      dependsOn = "^parserConfig.splitDetails",
	      triggeredByValue = "true",
	      displayPosition = 20,
	      group = "DETAILS"
	  )
	public String separator;
	
	@ConfigDef(
	      required = false,
	      type = ConfigDef.Type.LIST,
	      defaultValue = "[\"/fieldSplit1\", \"/fieldSplit2\"]",
	      label = "New Split Fields",
	      description="New fields to pass split data. The last field includes any remaining unsplit data.",
	  	  dependencies = {
		  		@Dependency(configName = "^parserConfig.splitDetails", triggeredByValues = {"true"}),
	  			@Dependency(configName = "detailsColumnHeaderType", triggeredByValues = {"NO_HEADER", "IGNORE_HEADER"})
		  },  
	      displayPosition = 30,
	      group = "DETAILS"
	  )
	public List<String> fieldPathsForSplits;

	@ConfigDef(
		      required = true,
		      type = ConfigDef.Type.MODEL,
		      defaultValue = "TO_ERROR",
		      label = "Not Enough Splits",
		      description="Action for data that has fewer splits than configured field paths",
		      displayPosition = 40,
		      group = "DETAILS"
		  )
	@ValueChooserModel(OnStagePreConditionFailureChooserValues.class)
	public OnStagePreConditionFailure onStagePreConditionFailure;


	@ConfigDef(
		      required = true,
		      type = ConfigDef.Type.MODEL,
		      defaultValue = "TO_LAST_FIELD",
		      label = "Too Many Splits",
		      description="Action for data that more splits than configured field paths",
		      displayPosition = 50,
		      group = "DETAILS"
		  )
	@ValueChooserModel(TooManySplitsActionChooserValues.class)
	public TooManySplitsAction tooManySplitsAction;

	@ConfigDef(
		      required = true,
		      type = ConfigDef.Type.MODEL,
		      defaultValue = "",
		      label = "Field for Remaining Splits",
		      description = "List field used to store any remaining splits",
		      displayPosition = 55,
		      dependsOn = "tooManySplitsAction",
		      triggeredByValue = "TO_LIST",
		      group = "DETAILS"
	)
	@FieldSelectorModel(singleValued = true)
	public String remainingSplitsPath;

	
}