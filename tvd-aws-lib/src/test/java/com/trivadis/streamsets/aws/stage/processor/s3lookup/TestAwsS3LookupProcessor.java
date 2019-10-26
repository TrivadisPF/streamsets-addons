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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.trivadis.streamsets.aws.stage.lib.s3.AwsRegion;
import com.trivadis.streamsets.aws.stage.lib.s3.S3ConfigBean;
import com.trivadis.streamsets.aws.stage.lib.s3.S3ConnectionSourceConfig;
import com.trivadis.streamsets.aws.stage.lib.s3.S3FileConfig;
import com.trivadis.streamsets.aws.stage.lib.s3.S3SSEConfigBean;
import com.trivadis.streamsets.aws.stage.processor.s3lookup.config.AwsS3LookupProcessorConfig;
import com.trivadis.streamsets.aws.stage.processor.s3lookup.config.DataFormatType;
import com.trivadis.streamsets.aws.util.AWSConfig;
import com.trivadis.streamsets.aws.util.ProxyConfig;

import _ss_com.com.google.common.collect.ImmutableList;
import _ss_com.streamsets.datacollector.util.Configuration;

public class TestAwsS3LookupProcessor {
	private final static String TEST_CONTAINER = "devk-gus-test";
	private final static String TEST_OBJECT_PATH = "row01-small.txt";
	private AwsS3LookupProcessorConfig config;
	private InputStream stream = null;
	private List<Record> input = null;

	private Record createRecordWithValueAndTemplate(String objectPath) {
		Record record = RecordCreator.create();
		Map<String, Field> map = new HashMap<>();
		map.put("objectPath", Field.create(objectPath));
		record.set(Field.create(map));
		return record;
	}

	/**
	 * Create the base config for WASBLookup
	 * @return
	 */
	private AwsS3LookupProcessorConfig getS3BaseConfig() {
		AwsS3LookupProcessorConfig config = new AwsS3LookupProcessorConfig();
		config.s3ConfigBean = new S3ConfigBean();
		config.s3ConfigBean.s3Config = new S3ConnectionSourceConfig();
		config.s3ConfigBean.s3Config.bucket = "devk-gus-test";
		config.s3ConfigBean.s3Config.region = AwsRegion.EU_CENTRAL_1;
		// not necessary if region is specified
		// config.s3ConfigBean.s3Config.endpoint = 
		config.s3ConfigBean.enableMetaData = false;
		config.s3ConfigBean.numberOfThreads = 1;

		config.s3ConfigBean.sseConfig = new S3SSEConfigBean();
		config.s3ConfigBean.sseConfig.useCustomerSSEKey = false;

		
		config.s3ConfigBean.proxyConfig = new ProxyConfig();
		config.s3ConfigBean.proxyConfig.connectionTimeout = 10;
		config.s3ConfigBean.proxyConfig.socketTimeout = 50;
		config.s3ConfigBean.proxyConfig.retryCount = 3;
		config.s3ConfigBean.proxyConfig.retryCount = 3;
		config.s3ConfigBean.proxyConfig.useProxy = false;
		
		
//		config.s3ConfigBean.s3FileConfig = new S3FileConfig();
		
		config.s3ConfigBean.s3Config.awsConfig = new AWSConfig();
		config.s3ConfigBean.s3Config.awsConfig.awsAccessKeyId = new CredentialValue() {
			@Override
			public String get() throws StageException {
				return "XXXXX";
			}
		};
		config.s3ConfigBean.s3Config.awsConfig.awsSecretAccessKey = new CredentialValue() {
			@Override
			public String get() throws StageException {
				return "XXXX";
			}
		};
	
		config.objectPath = TEST_OBJECT_PATH;
		config.fieldWithObjectPath = "/objectPath";
		config.outputField = "/value";
	
		return config;
	}

	@Before
	public void setup() {
		File dir = new File("target", UUID.randomUUID().toString());
		dir.mkdirs();
		Configuration.setFileRefsBaseDir(dir);

		config = getS3BaseConfig();
		
		Record record = createRecordWithValueAndTemplate(TEST_OBJECT_PATH);
		input = ImmutableList.of(record);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAsBlob() throws StageException, IOException {
		config.objectPathFromField = true;
		config.dataFormat = DataFormatType.AS_BLOB;

		AwsS3LookupDProcessor processor = new AwsS3LookupDProcessor();
		processor.config = config;

		ProcessorRunner runner = new ProcessorRunner.Builder(AwsS3LookupDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("output")
				.build();

		runner.runInit();

		List<Record> op = null;
		try {

			StageRunner.Output output = runner.runProcess(input);

			op = output.getRecords().get("output");
		} finally {
			runner.runDestroy();
		}
		
		assertNotNull(op);
		assertEquals(op.size(), 1);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testMultipleLines() throws StageException, IOException {
		config.objectPathFromField = true;
		config.dataFormat = DataFormatType.AS_RECORDS;

		AwsS3LookupDProcessor processor = new AwsS3LookupDProcessor();
		processor.config = config;

		ProcessorRunner runner = new ProcessorRunner.Builder(AwsS3LookupDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("output")
				.build();

		runner.runInit();

		List<Record> op = null;
		try {
			StageRunner.Output output = runner.runProcess(input);

			op = output.getRecords().get("output");
		} finally {
			runner.runDestroy();
		}
		
		assertNotNull(op);
		assertEquals(op.size(), 9376);		
	}

}
