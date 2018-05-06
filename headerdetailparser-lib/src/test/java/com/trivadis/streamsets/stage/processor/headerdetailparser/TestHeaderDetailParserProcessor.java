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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
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
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.DataFormatType;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.DetailsColumnHeaderType;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserDetailsConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserHeaderConfig;

import _ss_com.com.google.common.collect.ImmutableList;
import _ss_com.streamsets.datacollector.util.Configuration;
import _ss_org.apache.commons.io.IOUtils;
import _ss_org.apache.commons.lang3.StringUtils;

public class TestHeaderDetailParserProcessor {
	private final static int NOF_HEADER_LINES = 15;
	private final static String TEST_FILE_WITH_HEADER_AND_DETAILS_HEADER = "with_header_and_details_col_header.txt";
	private final static String TEST_FILE_WITH_HEADER_AND_NO_DETAILS_HEADER = "with_header_and_NO_details_col_header.txt";
	private final static String TEST_FILE_EMPTY = "empty.txt";

	private HeaderDetailParserDProcessor processor;

	private ProcessorRunner runner;

	private HeaderDetailParserConfig getBaseParserConfig() {
		HeaderDetailParserConfig config = new HeaderDetailParserConfig();
		config.inputDataFormat = DataFormatType.AS_BLOB;
		config.fieldPathToParse = "/value";
		config.keepOriginalFields = false;
		config.outputField = null;
		config.detailLineField = "/detail";
		config.splitDetails = false;
		return config;
	}


	private Record createRecordWithValueAndTemplate(String value) {
		Record record = RecordCreator.create();
		Map<String, Field> map = new HashMap<>();
		map.put("value", Field.create(value));
		record.set(Field.create(map));
		return record;
	}

	private List<Record> prepareInput(String inputFile) throws IOException {
		InputStream stream = null;

		stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(inputFile);
		String value = IOUtils.toString(stream);

		Record r0 = createRecordWithValueAndTemplate(value);
		List<Record> input = ImmutableList.of(r0);

		return input;
	}
	
	private HeaderDetailParserHeaderConfig getHeaderConfig(String headerDetailSpeparator, Integer nofHeaderLines) {
		HeaderDetailParserHeaderConfig headerConfig = new HeaderDetailParserHeaderConfig();

		headerConfig.headerExtractorConfigs = new ArrayList<>();
		HeaderExtractorConfig config = null; 
		
		config = new HeaderExtractorConfig();
		config.key = null;
		config.lineNumber = 2;
		config.regex = "(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)";
		headerConfig.headerExtractorConfigs.add(config);

		config = new HeaderExtractorConfig();
		config.key = null;
		config.lineNumber = 3;
		config.regex = "(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)";
		headerConfig.headerExtractorConfigs.add(config);
	
		headerConfig.headerDetailSeparator = headerDetailSpeparator;
		headerConfig.nofHeaderLines = nofHeaderLines;

		return headerConfig;
	}

	@Before
	public void setup() throws StageException {
		File dir = new File("target", UUID.randomUUID().toString());
		dir.mkdirs();
		Configuration.setFileRefsBaseDir(dir);

		processor = new HeaderDetailParserDProcessor();
		processor.parserConfig = getBaseParserConfig();
		processor.headerConfig = new HeaderDetailParserHeaderConfig();
		processor.detailsConfig = new HeaderDetailParserDetailsConfig();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testHeaderAndDetailAsOne_headerSplitOnRegex() throws StageException, IOException {

		// prepare parser config

		// prepare header config
		processor.headerConfig = getHeaderConfig("^-----", null);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.IGNORE_HEADER;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> header = null;
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITH_HEADER_AND_DETAILS_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			header = output.getRecords().get("header");
			headerDetails = output.getRecords().get("headerDetails");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(1, header.size());
		assertEquals("ROW01", header.get(0).get("/Location").getValueAsString());
		assertEquals("ROW01", header.get(0).get("/Position").getValueAsString());
		assertEquals(16, headerDetails.size());
		assertEquals("11:02:12.000", StringUtils.substring(headerDetails.get(0).get("/detail").getValueAsString(), 0, 12));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testHeaderAndDetailAsOne_headerSplitOnNofLines() throws StageException, IOException {

		// prepare parser config

		// prepare header config
		processor.headerConfig = getHeaderConfig(null, NOF_HEADER_LINES);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.IGNORE_HEADER;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> op = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITH_HEADER_AND_DETAILS_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			op = output.getRecords().get("headerDetails");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(16, op.size());
		assertEquals("11:02:12.000", StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void testHeaderAndDetailAsOne_headerSplitOnNofLinesZERO() throws StageException, IOException {

		// prepare parser config

		// prepare header config
		processor.headerConfig = getHeaderConfig("^-----", 0);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.IGNORE_HEADER;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> header = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITH_HEADER_AND_DETAILS_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			// get the header output
			header = output.getRecords().get("header");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(0, header.size());
	}	
	
	@Test
	@SuppressWarnings("unchecked")
	public void testHeaderAndDetailAsOne_headerSplitOnNofLines_NoDetailColHeader() throws StageException, IOException {

		// prepare parser config

		// prepare header config
		processor.headerConfig = getHeaderConfig(null, NOF_HEADER_LINES);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.NO_HEADER;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> op = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITH_HEADER_AND_NO_DETAILS_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			op = output.getRecords().get("headerDetails");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(16, op.size());
		assertEquals("11:02:12.000", StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));
	}	
	
	@Test
	@SuppressWarnings("unchecked")
	public void testEmptyFile() throws StageException, IOException {

		// prepare parser config

		// prepare header config
		processor.headerConfig = getHeaderConfig(null, NOF_HEADER_LINES);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.IGNORE_HEADER;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> op = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_EMPTY);
			StageRunner.Output output = runner.runProcess(input);

			op = output.getRecords().get("headerDetails");
	//		fail("An error was expected");
		} catch (Exception e) {
			// expected
		} finally {
			runner.runDestroy();
		}

	}

	
	@Test
	@SuppressWarnings("unchecked")
	public void testLineSplit() throws StageException, IOException {
		InputStream stream = null;

		stream = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream("ROW01_N02_01B0B628_RB_20Hz_20161010_110212_small.txt");
		String value = IOUtils.toString(stream);

		File dir = new File("target", UUID.randomUUID().toString());
		dir.mkdirs();
		Configuration.setFileRefsBaseDir(dir);

		// UtahParserDProcessor processor = new UtahParserDProcessor();

		List<HeaderExtractorConfig> headerExtractorConfigs = new ArrayList<>();
		HeaderExtractorConfig config = new HeaderExtractorConfig();
		config.key = null;
		config.lineNumber = 3;

		config.regex = "(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)";
		headerExtractorConfigs.add(config);

		ProcessorRunner runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, null)
				.addConfiguration("fieldPathToParse", "/value").addConfiguration("dataFormat", DataFormatType.AS_BLOB)
				.addConfiguration("keepOriginalFields", false)
				.addConfiguration("headerExtractorConfigs", headerExtractorConfigs)
				.addConfiguration("headerDetailSeparator", "^-----")
				// .addConfiguration("nofHeaderLines", 15)
				.addConfiguration("outputField", "/").addConfiguration("detailLineField", "/detail")
				.addConfiguration("splitDetails", true).addConfiguration("separator", ",")
				.addConfiguration("headerType", DetailsColumnHeaderType.USE_HEADER)
				.setExecutionMode(ExecutionMode.STANDALONE).setResourcesDir("/tmp").addOutputLane("output").build();

		runner.runInit();

		try {
			Record r0 = createRecordWithValueAndTemplate(value);
			List<Record> input = ImmutableList.of(r0);
			StageRunner.Output output = runner.runProcess(input);

			List<Record> op = output.getRecords().get("output");

			System.out.println(op.get(0).get("/detail").getValueAsListMap());

			assertEquals(16, op.size());
			// assertEquals("11:02:12.000",
			// StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));

		} finally {
			runner.runDestroy();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testLineSplit2() throws StageException, IOException {
		InputStream stream = null;

		stream = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream("ROW01_N02_01B0B628_RB_20Hz_20161010_110212_small.txt");
		String value = IOUtils.toString(stream);

		File dir = new File("target", UUID.randomUUID().toString());
		dir.mkdirs();
		Configuration.setFileRefsBaseDir(dir);

		// UtahParserDProcessor processor = new UtahParserDProcessor();

		List<HeaderExtractorConfig> headerExtractorConfigs = new ArrayList<>();
		HeaderExtractorConfig config = new HeaderExtractorConfig();
		config.key = null;
		config.lineNumber = 3;

		config.regex = "(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)";
		headerExtractorConfigs.add(config);

		ProcessorRunner runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, null)
				.addConfiguration("fieldPathToParse", "/value").addConfiguration("dataFormat", DataFormatType.AS_BLOB)
				.addConfiguration("keepOriginalFields", false)
				.addConfiguration("headerExtractorConfigs", headerExtractorConfigs)
				.addConfiguration("headerDetailSeparator", "^-----")
				// .addConfiguration("nofHeaderLines", 15)
				.addConfiguration("outputField", "/").addConfiguration("detailLineField", "/")
				.addConfiguration("splitDetails", true).addConfiguration("separator", ",")
				.addConfiguration("headerType", DetailsColumnHeaderType.USE_HEADER)
				.setExecutionMode(ExecutionMode.STANDALONE).setResourcesDir("/tmp").addOutputLane("output").build();

		runner.runInit();

		try {
			Record r0 = createRecordWithValueAndTemplate(value);
			List<Record> input = ImmutableList.of(r0);
			StageRunner.Output output = runner.runProcess(input);

			List<Record> op = output.getRecords().get("output");

			System.out.println(op.get(0));

			assertEquals(16, op.size());
			// assertEquals("11:02:12.000",
			// StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));

		} finally {
			runner.runDestroy();
		}
	}
}
