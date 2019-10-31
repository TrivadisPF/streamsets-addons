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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserDetailsConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserHeaderConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserOutputStreams;


@StageDef(version = 1, 
			label = "Header/Detail Parser Processor", 
			description = "", 
			icon = "headerdetailparser.png", 
			outputStreams = HeaderDetailParserOutputStreams.class,
//			producesEvents = true,
			onlineHelpRefUrl = "")
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class HeaderDetailParserDProcessor extends HeaderDetailParserProcessor {

	@ConfigDefBean()
	public HeaderDetailParserConfig parserConfig;

	@ConfigDefBean()
	public HeaderDetailParserHeaderConfig headerConfig;

	@ConfigDefBean()
	public HeaderDetailParserDetailsConfig detailsConfig;

	@Override
	public HeaderDetailParserConfig getParserConfig() {
		return parserConfig;
	}

	@Override
	public HeaderDetailParserHeaderConfig getHeaderConfig() {
		return headerConfig;
	}

	@Override
	public HeaderDetailParserDetailsConfig getDetailsConfig() {
		return detailsConfig;
	}


}
