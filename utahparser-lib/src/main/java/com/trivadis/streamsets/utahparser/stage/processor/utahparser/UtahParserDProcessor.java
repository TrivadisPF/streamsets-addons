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
package com.trivadis.streamsets.utahparser.stage.processor.utahparser;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;


@StageDef(version = 1, label = "Utah Parser Processor", description = "", icon = "utahparser.png", onlineHelpRefUrl = "")
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class UtahParserDProcessor extends UtahParserProcessor {


	@ConfigDef(required = true, 
			type = ConfigDef.Type.MODEL, 
			defaultValue = "", 
			label = "Field to Parse", 
			description = "The field containing the semi-structured text data to be parsed by the Utah-Parser", 
			displayPosition = 10, 
			group = "PARSER")
	@FieldSelectorModel(singleValued = true)
	public String fieldPathToParse;
	
	  @ConfigDef(
		      required = true,
		      type = ConfigDef.Type.BOOLEAN,
		      defaultValue = "true",
		      label = "Keep Original Field",
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
	
	@ConfigDef(required = true, 
			type = ConfigDef.Type.TEXT, 
			defaultValue = "default", 
			label = "Utah Parser Template in XML format", 
			displayPosition = 40, 
			group = "PARSER")
	public String template;
	

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
	public String getTemplate() {
		return template;
	}

}
