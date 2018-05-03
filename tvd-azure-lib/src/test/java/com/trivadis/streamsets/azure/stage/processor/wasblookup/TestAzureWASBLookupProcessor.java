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
package com.trivadis.streamsets.azure.stage.processor.wasblookup;

import static org.junit.Assert.*;

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
import com.trivadis.streamsets.azure.stage.lib.wasb.AzureConfig;
import com.trivadis.streamsets.azure.stage.processor.wasblookup.config.AzureWASBLookupProcessorConfig;
import com.trivadis.streamsets.azure.stage.processor.wasblookup.config.DataFormatType;

import _ss_com.com.google.common.collect.ImmutableList;
import _ss_com.streamsets.datacollector.util.Configuration;
import _ss_org.apache.commons.io.IOUtils;

public class TestAzureWASBLookupProcessor {
	private final static String TEST_CONTAINER = "raw-data";
	private final static String TEST_OBJECT_PATH = "orsted/2018-04-20-21/ROW01_N02_01B0B628_RB_20Hz_20161010_110212-11.txt.ready";
	private AzureWASBLookupProcessorConfig config;
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
	private AzureWASBLookupProcessorConfig getWASBBaseConfig() {
		AzureWASBLookupProcessorConfig config = new AzureWASBLookupProcessorConfig();
		config.azureConfig = new AzureConfig();
		config.azureConfig.accountName = new CredentialValue() {
			@Override
			public String get() throws StageException {
				return "porststac001";
			}
		};
		config.azureConfig.accountKey = new CredentialValue() {
			@Override
			public String get() throws StageException {
				return "KlfnPZyUAKdmll/h+XCUISX6mhGtqZpASt/OwqPKNEj1jz6R1Ntxu0DpEl+SdtFoLlJLKalBhR5td6n8W2jElg==";
			}
		};

		config.container = TEST_CONTAINER;
		config.fieldWithContainer = "/containerName";
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

		config = getWASBBaseConfig();
		
		Record record = createRecordWithValueAndTemplate(TEST_OBJECT_PATH);
		input = ImmutableList.of(record);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testAsBlob() throws StageException, IOException {
		config.containerFromField = false;
		config.objectPathFromField = true;
		config.dataFormat = DataFormatType.AS_BLOB;

		AzureWASBLookupDProcessor processor = new AzureWASBLookupDProcessor();
		processor.config = config;

		ProcessorRunner runner = new ProcessorRunner.Builder(AzureWASBLookupDProcessor.class, processor)
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
		config.containerFromField = false;
		config.objectPathFromField = true;
		config.dataFormat = DataFormatType.AS_RECORDS;

		AzureWASBLookupDProcessor processor = new AzureWASBLookupDProcessor();
		processor.config = config;

		ProcessorRunner runner = new ProcessorRunner.Builder(AzureWASBLookupDProcessor.class, processor)
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
