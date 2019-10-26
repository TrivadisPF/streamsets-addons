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
package com.trivadis.streamsets.aws.stage.processor.s3lookup;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.trivadis.streamsets.aws.stage.origin.s3.Groups;
import com.trivadis.streamsets.aws.stage.processor.s3lookup.config.AwsS3LookupProcessorConfig;

@StageDef(
		  version = 1,
		  label = "AWS S3 Lookup",
		  description = "Lookup a blob from AWS S3 Blob Storage.",
		  icon = "s3.png",
		  onlineHelpRefUrl =""
		)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class AwsS3LookupDProcessor extends AwsS3LookupProcessor {
	@ConfigDefBean()
	public AwsS3LookupProcessorConfig config;

	@Override
	public AwsS3LookupProcessorConfig getConfig() {
		return config;
	}

}
