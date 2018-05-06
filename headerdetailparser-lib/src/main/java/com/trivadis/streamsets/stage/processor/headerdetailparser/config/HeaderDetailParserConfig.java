package com.trivadis.streamsets.stage.processor.headerdetailparser.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;

public class HeaderDetailParserConfig {
	@ValueChooserModel(InputDataFormatChooserValues.class)
	@ConfigDef(
		    required = true,
		    type = ConfigDef.Type.MODEL,
		    defaultValue = "AS_RECORDS",
		    label = "Input Data Format",
		    description = "How should the output be produced.",
		    group = "PARSER",
		    displayPosition = 0
		  )
	public DataFormatType inputDataFormat;
	@FieldSelectorModel(singleValued = true)
	@ConfigDef(required = true, 
			type = ConfigDef.Type.MODEL, 
			defaultValue = "", 
			label = "Field to Parse", 
			description = "The field containing the semi-structured text data to be parsed by the Utah-Parser", 
			dependencies = {
					@Dependency(configName = "inputDataFormat", triggeredByValues = {"AS_RECORDS", "AS_BLOB"})
			},  
			displayPosition = 10, 
			group = "PARSER")
	public String fieldPathToParse;
	@ConfigDef(
		      required = true,
		      type = ConfigDef.Type.BOOLEAN,
		      defaultValue = "true",
		      label = "Keep Original Fields",
		      description = "Whether the original fields should be kept. " +
		          "If this is set, the root field of the record must be a Map or List Map.",
		      displayPosition = 20,
		      group = "PARSER"
		  )
	public boolean keepOriginalFields;
	@ConfigDef(
			required = true,
			type = ConfigDef.Type.STRING,
			defaultValue = "/",
			label = "Output Field",
			description="Output field into which the unstructured text will be parsed. Use empty value to write directly to root of the record.",
			dependsOn = "keepOriginalFields",
			triggeredByValue = "true",
			displayPosition = 30,
			group = "PARSER"
			)
	public String outputField;
	@ConfigDef(
		      required = true,
		      type = ConfigDef.Type.STRING,
		      defaultValue = "detail",
		      label = "Detail Line Field",
		      description="Output field into which the detail line will parsed.",
		      displayPosition = 60,
		      group = "PARSER"
		  )
	public String detailLineField;
	@ConfigDef(
		      required = true,
		      type = ConfigDef.Type.BOOLEAN,
		      defaultValue = "false",
		      label = "Split Details?",
		      description="Should the detail line be split or returned as is.",
		      displayPosition = 70,
		      group = "PARSER"
		  )
	public boolean splitDetails;

}