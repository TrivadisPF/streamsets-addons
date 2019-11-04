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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.OnStagePreConditionFailure;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.TooManySplitsAction;

import _ss_com.com.google.common.collect.ImmutableList;
import _ss_com.streamsets.datacollector.util.Configuration;
import _ss_org.apache.commons.io.IOUtils;
import _ss_org.apache.commons.lang3.StringUtils;

public class TestHeaderDetailParserProcessor {
	private final static int NOF_HEADER_LINES = 15;
	private final static String TEST_FILE_WITH_HEADER_AND_DETAILS_HEADER = "with_header_and_details_col_header.txt";
	private final static String TEST_FILE_WITH_HEADER_AND_DETAILS_HEADER_TSV = "with_header_and_details_col_header-tsv.txt";
	private final static String TEST_FILE_WITH_HEADER_AND_NO_DETAILS_HEADER = "with_header_and_NO_details_col_header.txt";
	private final static String TEST_FILE_WITH_HEADER_LEFT_AND_DETAILS_HEADER = "with_header_left_and_details_col_header.txt";
	private final static String TEST_FILE_WITHOUT_HEADER_BUT_WITH_DETAILS_COL_HEADER = "without_header_but_with_details_col_header.txt";
	private final static String TEST_FILE_WITHOUT_HEADER_BUT_WITH_DETAILS_COL_HEADER_3COLS = "without_header_but_with_details_col_header_3cols.txt";
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
	
	private HeaderDetailParserHeaderConfig getHeaderParserConfig() {
		HeaderDetailParserHeaderConfig config = new HeaderDetailParserHeaderConfig();
		return config;
	}

	private HeaderDetailParserDetailsConfig getDetailsParserConfig() {
		HeaderDetailParserDetailsConfig config = new HeaderDetailParserDetailsConfig();
		config.fieldPathsForSplits = null;
		config.separator = ",";
		config.separatorAsRegex = true;
		config.onStagePreConditionFailure = OnStagePreConditionFailure.TO_ERROR;
		config.tooManySplitsAction = TooManySplitsAction.TO_LAST_FIELD;
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

	
	private HeaderDetailParserHeaderConfig getHeaderConfigNoHeaders(String headerDetailSpeparator, Integer nofHeaderLines) {
		HeaderDetailParserHeaderConfig headerConfig = new HeaderDetailParserHeaderConfig();

		headerConfig.headerExtractorConfigs = new ArrayList<>();
			
		headerConfig.headerDetailSeparator = headerDetailSpeparator;
		headerConfig.nofHeaderLines = nofHeaderLines;

		return headerConfig;
	}

	private HeaderDetailParserHeaderConfig getHeaderConfigHeaderLeft(String headerDetailSpeparator, Integer nofHeaderLines) {
		HeaderDetailParserHeaderConfig headerConfig = new HeaderDetailParserHeaderConfig();

		headerConfig.headerExtractorConfigs = new ArrayList<>();
		HeaderExtractorConfig config = null; 
		
		config = new HeaderExtractorConfig();
		config.key = null;
		config.lineNumber = 2;
		config.regex = "(\\w[^\\t]+[A-Za-z\\.])[\"\\t]*(\\w[^\\t]+\\w)";
		headerConfig.headerExtractorConfigs.add(config);

		config = new HeaderExtractorConfig();
		config.key = null;
		config.lineNumber = 3;
		config.regex = "(\\w[^\\t]+[A-Za-z\\.])[\"\\t]*(\\w[^\\t]+\\w)";
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
		processor.headerConfig =  getHeaderParserConfig();
		processor.detailsConfig =  getDetailsParserConfig();
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_headerSplitOnRegex() throws StageException, IOException {

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

		assertFalse("/value should be part of output, due to keepOriginalFields", headerDetails.get(0).has("/value"));

		assertEquals("ROW01", header.get(0).get("/Location").getValueAsString());
		assertEquals("ROW01", header.get(0).get("/Position").getValueAsString());
		assertEquals(18, headerDetails.size());
		assertEquals("11:02:12.000", StringUtils.substring(headerDetails.get(0).get("/detail").getValueAsString(), 0, 12));
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_headerSplitOnRegexKeepOriginalFields() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.keepOriginalFields = true;
		
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

		assertTrue("/value should be part of output, due to keepOriginalFields", headerDetails.get(0).has("/value"));

		assertEquals("ROW01", header.get(0).get("/Location").getValueAsString());
		assertEquals("ROW01", header.get(0).get("/Position").getValueAsString());
		assertEquals(18, headerDetails.size());
		assertEquals("11:02:12.000", StringUtils.substring(headerDetails.get(0).get("/detail").getValueAsString(), 0, 12));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void test_headerSplitOnNofLines() throws StageException, IOException {

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
		assertEquals(18, op.size());
		assertEquals("11:02:12.000", StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_headerSplitOnNofLinesZERO() throws StageException, IOException {

		// prepare parser config

		// prepare header config
		processor.headerConfig = getHeaderConfig("^-----", 0);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.USE_HEADER;
		
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
	public void test_headerSplitOnNofLines_NoDetailColHeader() throws StageException, IOException {

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
		assertEquals(18, op.size());
		assertEquals("11:02:12.000", StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));
	}	
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_headerSplitOnNofLines_splitDetailsUsingColHeader() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;
		processor.parserConfig.detailLineField = "/detail";

		// prepare header config
		processor.headerConfig = getHeaderConfig(null, NOF_HEADER_LINES);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.USE_HEADER;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITH_HEADER_AND_DETAILS_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			headerDetails = output.getRecords().get("headerDetails");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(18, headerDetails.size());
		assertEquals("ROW01", headerDetails.get(0).get("/Location").getValueAsString());
		assertEquals("ROW01", headerDetails.get(0).get("/Position").getValueAsString());
		assertEquals("11:02:12.000 10/10/2016", headerDetails.get(0).get("/detail").getValueAsListMap().get("/Time and Date").getValueAsString());
		assertEquals("500.2231", headerDetails.get(0).get("/detail").getValueAsListMap().get("/PT100-0").getValueAsString());
		assertEquals("-25013.7066", headerDetails.get(0).get("/detail").getValueAsListMap().get("/sghalf47").getValueAsString());
	}	

	@Test
	@SuppressWarnings("unchecked")
	public void test_headerSplitOnNofLines_splitDetailsUsingColHeaderNoRegexpSplitterForDetails() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;
		processor.parserConfig.detailLineField = "/detail";

		// prepare header config
		processor.headerConfig = getHeaderConfig(null, NOF_HEADER_LINES);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.USE_HEADER;
		processor.detailsConfig.separatorAsRegex = false;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITH_HEADER_AND_DETAILS_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			headerDetails = output.getRecords().get("headerDetails");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(18, headerDetails.size());
		assertEquals("ROW01", headerDetails.get(0).get("/Location").getValueAsString());
		assertEquals("ROW01", headerDetails.get(0).get("/Position").getValueAsString());
		assertEquals("11:02:12.000 10/10/2016", headerDetails.get(0).get("/detail").getValueAsListMap().get("/Time and Date").getValueAsString());
		assertEquals("500.2231", headerDetails.get(0).get("/detail").getValueAsListMap().get("/PT100-0").getValueAsString());
		assertEquals("-25013.7066", headerDetails.get(0).get("/detail").getValueAsListMap().get("/sghalf47").getValueAsString());
	}	
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_headerSplitOnNofLinesTSV() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;

		// prepare header config
		processor.headerConfig = getHeaderConfig(null, NOF_HEADER_LINES);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.USE_HEADER;
		processor.detailsConfig.separator = "\\t";
		processor.detailsConfig.separatorAsRegex = false;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITH_HEADER_AND_DETAILS_HEADER_TSV);
			StageRunner.Output output = runner.runProcess(input);

			headerDetails = output.getRecords().get("headerDetails");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(18, headerDetails.size());
		assertEquals("11:02:12.000 10/10/2016", headerDetails.get(0).get("/detail").getValueAsListMap().get("/Time and Date").getValueAsString());
		assertEquals("500.2231", headerDetails.get(0).get("/detail").getValueAsListMap().get("/PT100-0").getValueAsString());
		assertEquals("-25013.7066", headerDetails.get(0).get("/detail").getValueAsListMap().get("/sghalf47").getValueAsString());
	}
	
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_noHeader_splitDetailsUsingColHeader() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;
		processor.parserConfig.detailLineField = "/detail";

		// prepare header config
		processor.headerConfig = getHeaderConfigNoHeaders(null, 0);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.USE_HEADER;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headers = null;
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITHOUT_HEADER_BUT_WITH_DETAILS_COL_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			headers = output.getRecords().get("header");
			headerDetails = output.getRecords().get("headerDetails");
			

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals("11:02:12.000 10/10/2016", headerDetails.get(0).get("/detail").getValueAsListMap().get("/Time and Date").getValueAsString());
		assertEquals("500.2231", headerDetails.get(0).get("/detail").getValueAsListMap().get("/PT100-0").getValueAsString());
		assertEquals("-25013.7066", headerDetails.get(0).get("/detail").getValueAsListMap().get("/sghalf47").getValueAsString());
	}	
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_noHeader_splitDetailsUsingFields() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;
		processor.parserConfig.detailLineField = "/detail";

		// prepare header config
		processor.headerConfig = getHeaderConfigNoHeaders(null, 0);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.IGNORE_HEADER;
		
		processor.detailsConfig.fieldPathsForSplits = new ArrayList<String>();
		processor.detailsConfig.fieldPathsForSplits.add("/timeAndDate");
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_0");
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_1");
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headers = null;
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITHOUT_HEADER_BUT_WITH_DETAILS_COL_HEADER_3COLS);
			StageRunner.Output output = runner.runProcess(input);

			headers = output.getRecords().get("header");
			headerDetails = output.getRecords().get("headerDetails");
			

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals("11:02:12.050 10/10/2016", headerDetails.get(0).get("/detail").getValueAsListMap().get("/timeAndDate").getValueAsString());
		assertEquals("500.2231", headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_0").getValueAsString());
		assertEquals("-500.0321", headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_1").getValueAsString());
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_noHeader_splitDetailsUsingTooManyFieldsAsNULL() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;
		processor.parserConfig.detailLineField = "/detail";

		// prepare header config
		processor.headerConfig = getHeaderConfigNoHeaders(null, 0);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.IGNORE_HEADER;
		
		processor.detailsConfig.onStagePreConditionFailure = OnStagePreConditionFailure.CONTINUE;
		processor.detailsConfig.tooManySplitsAction = TooManySplitsAction.TO_LAST_FIELD;
		
		processor.detailsConfig.fieldPathsForSplits = new ArrayList<String>();
		processor.detailsConfig.fieldPathsForSplits.add("/timeAndDate");
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_0");
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_1");
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_2");		// does not exist in data
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_3");		// does not exist in data
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headers = null;
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITHOUT_HEADER_BUT_WITH_DETAILS_COL_HEADER_3COLS);
			StageRunner.Output output = runner.runProcess(input);

			headers = output.getRecords().get("header");
			headerDetails = output.getRecords().get("headerDetails");
			

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals("11:02:12.050 10/10/2016", headerDetails.get(0).get("/detail").getValueAsListMap().get("/timeAndDate").getValueAsString());
		assertEquals("500.2231", headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_0").getValueAsString());
		assertEquals("-500.0321", headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_1").getValueAsString());
		assertNull(headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_2").getValueAsString());
		assertNull(headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_3").getValueAsString());
	}	

	@Test
	@SuppressWarnings("unchecked")
	public void test_noHeader_splitDetailsUsingTooManyFieldsAsBlankString() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;
		processor.parserConfig.detailLineField = "/detail";

		// prepare header config
		processor.headerConfig = getHeaderConfigNoHeaders(null, 0);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.IGNORE_HEADER;
		
		processor.detailsConfig.onStagePreConditionFailure = OnStagePreConditionFailure.CONTINUE;
		processor.detailsConfig.tooManySplitsAction = TooManySplitsAction.TO_LAST_FIELD;
		
		processor.detailsConfig.fieldPathsForSplits = new ArrayList<String>();
		processor.detailsConfig.fieldPathsForSplits.add("/timeAndDate");
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_0");
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_1");
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_2");		// does not exist in data
		processor.detailsConfig.fieldPathsForSplits.add("/pt100_3");		// does not exist in data
		processor.detailsConfig.useNULLforFieldsWithoutSplitValue = false;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headers = null;
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITHOUT_HEADER_BUT_WITH_DETAILS_COL_HEADER_3COLS);
			StageRunner.Output output = runner.runProcess(input);

			headers = output.getRecords().get("header");
			headerDetails = output.getRecords().get("headerDetails");
			

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals("11:02:12.050 10/10/2016", headerDetails.get(0).get("/detail").getValueAsListMap().get("/timeAndDate").getValueAsString());
		assertEquals("500.2231", headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_0").getValueAsString());
		assertEquals("-500.0321", headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_1").getValueAsString());
		assertEquals("", headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_2").getValueAsString());
		assertEquals("", headerDetails.get(0).get("/detail").getValueAsListMap().get("/pt100_3").getValueAsString());
	}	

	
	@Test
	@SuppressWarnings("unchecked")
	public void test_noHeader_ignoreDetailColHeaderUsingNofHeaders() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;
		processor.parserConfig.detailLineField = "/detail";

		// prepare header config
		processor.headerConfig = getHeaderConfigNoHeaders(null, 1);

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.NO_HEADER;
		processor.detailsConfig.separator = ",";
		processor.detailsConfig.fieldPathsForSplits = Arrays.asList(StringUtils.split("/Time and Date,/PT100-0,/PT100-1,/PT100-2,/PT100-3,/PT100-4,/PT100-5,/PT100-6,/PT100-7,/PT100-8,/PT100-9,/PT100-10,/PT100-11,/PT100-12,/PT100-13,/PT100-14,/PT100-15,/AI0,/AI1,/AI2,/AI3,/AI4,/AI5,/AI6,/AI7,/AI8,/AI9,/AI10,/AI11,/AI12,/AI13,/AI14,/AI15,/SGQT0,/SGQT1,/SGQT2,/SGQT3,/SGQT4,/SGQT5,/SGQT6,/SGQT7,/SGQT8,/SGQT9,/SGQT10,/SGQT11,/SGQT12,/SGQT13,/SGQT14,/SGQT15,/SGQT16,/SGQT17,/SGQT18,/SGQT19,/SGQT20,/SGQT21,/SGQT22,/SGQT23,/sghalf0,/sghalf1,/sghalf2,/sghalf3,/sghalf4,/sghalf5,/sghalf6,/sghalf7,/sghalf8,/sghalf9,/sghalf10,/sghalf11,/sghalf12,/sghalf13,/sghalf14,/sghalf15,/sghalf16,/sghalf17,/sghalf18,/sghalf19,/sghalf20,/sghalf21,/sghalf22,/sghalf23,/sghalf24,/sghalf25,/sghalf26,/sghalf27,/sghalf28,/sghalf29,/sghalf30,/sghalf31,/sghalf32,/sghalf33,/sghalf34,/sghalf35,/sghalf36,/sghalf37,/sghalf38,/sghalf39,/sghalf40,/sghalf41,/sghalf42,/sghalf43,/sghalf44,/sghalf45,/sghalf46,/sghalf47",","));
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headers = null;
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITHOUT_HEADER_BUT_WITH_DETAILS_COL_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			headers = output.getRecords().get("header");
			headerDetails = output.getRecords().get("headerDetails");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(0, headers.size());
		Record r = headerDetails.get(0);
		assertEquals("11:02:12.000 10/10/2016", headerDetails.get(0).get("/detail").getValueAsListMap().get("/Time and Date").getValueAsString());
		assertEquals("500.2231", headerDetails.get(0).get("/detail").getValueAsListMap().get("/PT100-0").getValueAsString());
		assertEquals("-25013.7066", headerDetails.get(0).get("/detail").getValueAsListMap().get("/sghalf47").getValueAsString());
	}	
	
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_headerLeftSplitOnNofLines_splitDetailsUsingColHeader() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;
		processor.parserConfig.detailLineField = "/detail";

		// prepare header config
		processor.headerConfig = getHeaderConfigHeaderLeft(null, 0);
		processor.headerConfig.nofHeaderLines = 0;

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.USE_HEADER;
		processor.detailsConfig.separator = "\\t";
		processor.detailsConfig.separatorAsRegex = true;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITH_HEADER_LEFT_AND_DETAILS_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			headerDetails = output.getRecords().get("headerDetails");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(31, headerDetails.size());
		assertEquals("ICL-02i/80", headerDetails.get(0).get("/Logger Type").getValueAsString());
		assertEquals("AK02691349", headerDetails.get(0).get("/Logger serial no.").getValueAsString());
		assertEquals("21-03-2017 07:02", headerDetails.get(0).get("/detail").getValueAsListMap().get("/Date time").getValueAsString());
		assertEquals("0.09634", headerDetails.get(0).get("/detail").getValueAsListMap().get("/Uac (V)").getValueAsString());
		assertEquals("6.44879", headerDetails.get(0).get("/detail").getValueAsListMap().get("/Iac (mA)").getValueAsString());
	}		
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_headerLeftSplitOnNofLines_splitDetailsIgnoreColHeader() throws StageException, IOException {

		// prepare parser config
		processor.parserConfig.splitDetails = true;
		processor.parserConfig.detailLineField = "/detail";

		// prepare header config
		processor.headerConfig = getHeaderConfigHeaderLeft(null, 0);
		processor.headerConfig.nofHeaderLines = 0;

		// prepare details config
		processor.detailsConfig.detailsColumnHeaderType = DetailsColumnHeaderType.IGNORE_HEADER;
		processor.detailsConfig.separator = "\\t";
		processor.detailsConfig.fieldPathsForSplits = Arrays.asList("/d1", "/d2", "/d3", "/d4", "/Datetime", "/uac", "/iac", "/jac", "/rs", "idc", "/jdc", "/edc", "/rr", "/rc", "/d", "power", "warnings");
		processor.detailsConfig.onStagePreConditionFailure = OnStagePreConditionFailure.CONTINUE;
		
		runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, processor)
				.setExecutionMode(ExecutionMode.STANDALONE)
				.setResourcesDir("/tmp")
				.addOutputLane("header").addOutputLane("headerDetails")
				.build();
		runner.runInit();

		// run the test
		List<Record> headerDetails = null;
		try {
			List<Record> input = prepareInput(TEST_FILE_WITH_HEADER_LEFT_AND_DETAILS_HEADER);
			StageRunner.Output output = runner.runProcess(input);

			headerDetails = output.getRecords().get("headerDetails");

		} finally {
			runner.runDestroy();
		}

		// assert
		assertEquals(31, headerDetails.size());
		assertEquals("ICL-02i/80", headerDetails.get(0).get("/Logger Type").getValueAsString());
		assertEquals("AK02691349", headerDetails.get(0).get("/Logger serial no.").getValueAsString());
		assertEquals("21-03-2017 07:02", headerDetails.get(0).get("/detail").getValueAsListMap().get("/Datetime").getValueAsString());
		assertEquals("0.09634", headerDetails.get(0).get("/detail").getValueAsListMap().get("/uac").getValueAsString());
		assertEquals("6.44879", headerDetails.get(0).get("/detail").getValueAsListMap().get("/iac").getValueAsString());
	}	
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_emptyFile() throws StageException, IOException {

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
			fail("An error was expected");
		} catch (Exception e) {
			// expected
		} finally {
			runner.runDestroy();
		}
	}

}
