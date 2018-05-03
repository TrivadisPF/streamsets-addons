/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.trivadis.streamsets.stage.processor.headerdetailparser;

import java.util.List;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.DataFormatChooserValues;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.DataFormatType;


@StageDef(version = 1, 
			label = "Header/Detail Parser Processor", 
			description = "", 
			icon = "headerdetailparser.png", 
			onlineHelpRefUrl = "")
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class HeaderDetailParserDProcessor extends HeaderDetailParserProcessor {

	@ConfigDef(
		    required = true,
		    type = ConfigDef.Type.MODEL,
		    defaultValue = "AS_RECORDS",
		    label = "Input Data Format",
		    description = "How should the output be produced.",
		    group = "PARSER",
		    displayPosition = 0
		  )
	@ValueChooserModel(DataFormatChooserValues.class)
	public DataFormatType dataFormat = DataFormatType.AS_RECORDS;
	
	@ConfigDef(required = true, 
			type = ConfigDef.Type.MODEL, 
			defaultValue = "", 
			label = "Field to Parse", 
			description = "The field containing the semi-structured text data to be parsed by the Utah-Parser", 
			dependencies = {
					@Dependency(configName = "dataFormat", triggeredByValues = {"AS_RECORDS", "AS_BLOB"})
			},  
			displayPosition = 10, 
			group = "PARSER")
	@FieldSelectorModel(singleValued = true)
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
			displayPosition = 30,
			group = "PARSER",
		    dependsOn = "keepOriginalFields",
		    triggeredByValue = "true"
			)
	public String outputField;

	@ConfigDef(
		      required = false,
		      type = ConfigDef.Type.MODEL,
		      defaultValue = "",
		      label = "Header Extractors",
		      description="A regular expression which extracts key as group 1 and value as group 2",
		      displayPosition = 40,
		      group = "PARSER"
		  )
	@ListBeanModel
	public List<HeaderExtractorConfig> headerExtractorConfigs;
	
	@ConfigDef(
		      required = false,
		      type = ConfigDef.Type.STRING,
		      defaultValue = "",
		      label = "Header/Detail Separator",
		      description="A regular expression which identifiey a given line as a spearator line between the headers and the detail lines",
		      displayPosition = 45,
		      group = "PARSER"
		  )
	public String headerDetailSeparator;	 

	@ConfigDef(
		      required = false,
		      type = ConfigDef.Type.NUMBER,
		      defaultValue = "",
		      label = "Number of Header Lines",
		      description="The number of header lines to remove",
		      displayPosition = 50,
		      group = "PARSER"
		  )

	public Integer nofHeaderLines;
	
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
/*
	@ConfigDef(
		      required = true,
		      type = ConfigDef.Type.STRING,
		      defaultValue = "detail",
		      label = "Detail Line Field",
		      description="Output field into which the detail line will parsed.",
		      displayPosition = 60,
		      group = "PARSER"
		  )
	public boolean split;	 
*/	


	@Override
	public String getFieldPathToParse() {
		return fieldPathToParse;
	}
	
	@Override
	public boolean isKeepOriginalFields() {
		return keepOriginalFields;
	}

	@Override
	public String getOutputField() {
		return outputField;
	}

	@Override
	public List<HeaderExtractorConfig> getHeaderExtractorConfigs() {
		return headerExtractorConfigs;
	}

	@Override
	public String getHeaderDetailSeparator() {
		return headerDetailSeparator;
	}

	@Override
	public Integer getNofHeaderLines() {
		return nofHeaderLines;
	}

	@Override
	public String getDetailLineField() {
		return detailLineField;
	}
	
	@Override
	public DataFormatType getDataFormat() {
		return dataFormat;
	}
}
