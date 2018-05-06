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
package com.trivadis.streamsets.azure.stage.processor.wasblookup.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Dependency;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.trivadis.streamsets.azure.stage.lib.wasb.AzureConfig;



public class AzureWASBLookupProcessorConfig {

  public static final String BEAN_PREFIX = "config.";
  public static final String WASB_CONFIG_PREFIX = BEAN_PREFIX + "wasbConfig.";

  @ConfigDefBean(groups = "WASB")
  public AzureConfig azureConfig;
      
  @ConfigDef(
		    required = true,
		    type = ConfigDef.Type.BOOLEAN,
		    label = "Container Name from Field?",
		    description = "Name of the container",
		    displayPosition = 10,
		    group = "WASB"
		  )
  public boolean containerFromField = true;

  @ConfigDef(
		    required = true,
		    type = ConfigDef.Type.MODEL,
		    label = "Field with Container",
		    description = "Name of the container",
				    dependencies = {
					    	  @Dependency(configName = "containerFromField", triggeredByValues = "true")
					    },  
		    displayPosition = 20,
		    group = "WASB"
		  )
  @FieldSelectorModel(singleValued = true)
  public String fieldWithContainer;  
  
  @ConfigDef(
		    required = true,
		    type = ConfigDef.Type.STRING,
		    label = "Container",
		    description = "Name of the container",
				    dependencies = {
					    	  @Dependency(configName = "containerFromField", triggeredByValues = "false")
					    },  
		    displayPosition = 20,
		    group = "WASB"
		  )
  public String container;  

  @ConfigDef(
		    required = true,
		    type = ConfigDef.Type.BOOLEAN,
		    label = "Object path from Field?",
		    description = "Name of the container",
		    displayPosition = 10,
		    group = "LOOKUP"
		  )
 public boolean objectPathFromField = true;
  
  @ConfigDef(
		    required = true,
		    type = ConfigDef.Type.MODEL,
		    label = "Field with Object Path",
		    description = "Path to object that will be looked up",
		    dependencies = {
		    	  @Dependency(configName = "objectPathFromField", triggeredByValues = "true")
		    },  
		    group = "LOOKUP",
		    displayPosition = 20
		  )

  @FieldSelectorModel(singleValued = true)
  public String fieldWithObjectPath;

  @ConfigDef(
		    required = true,
		    type = ConfigDef.Type.STRING,
		    label = "Object Path",
		    description = "Path to object that will be looked up",
		    dependencies = {
					    	  @Dependency(configName = "objectPathFromField", triggeredByValues = "false")
			},  
		    group = "LOOKUP",
		    displayPosition = 20
		  )

  public String objectPath;  
  

  @ConfigDef(
		    required = true,
		    type = ConfigDef.Type.MODEL,
		    defaultValue = "AS_RECORDS",
		    label = "Output Data Format",
		    description = "How should the output be produced.",
		    group = "DATA_FORMAT",
		    displayPosition = 40
		  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormatType dataFormat = DataFormatType.AS_RECORDS;

  @ConfigDef(
		    required = true,
		    type = ConfigDef.Type.STRING,
		    defaultValue = "/",
		    label = "Output Field",
		    description = "Use an existing field or a new field. Using an existing field overwrites the original value",
		    dependencies = {
					    	  @Dependency(configName = "dataFormat", triggeredByValues = {"AS_BLOB", "AS_RECORDS"})
			},  
		    group = "DATA_FORMAT",
		    displayPosition = 50
		  )
  public String outputField = "/";
}
