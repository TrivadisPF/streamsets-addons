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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.DataFormatType;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderType;

import _ss_com.com.google.common.collect.ImmutableList;
import _ss_com.streamsets.datacollector.util.Configuration;
import _ss_org.apache.commons.io.IOUtils;
import _ss_org.apache.commons.lang3.StringUtils;

public class TestHeaderDetailParserProcessor {
	
  private Record createRecordWithValueAndTemplate(String value) {
		    Record record = RecordCreator.create();
		    Map<String, Field> map = new HashMap<>();
		    map.put("value", Field.create(value));
		    record.set(Field.create(map));
		    return record;
		  }
	  
  @Test
  @SuppressWarnings("unchecked")
  public void testProcessor() throws StageException, IOException {
	InputStream stream = null;
	
	stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("ROW01_N02_01B0B628_RB_20Hz_20161010_110212_small.txt");
	String value = IOUtils.toString(stream);

	File dir = new File("target", UUID.randomUUID().toString());
    dir.mkdirs();
    Configuration.setFileRefsBaseDir(dir);

	
	//UtahParserDProcessor processor = new UtahParserDProcessor();
    
    List<HeaderExtractorConfig> headerExtractorConfigs = new ArrayList<>();
    HeaderExtractorConfig config = new HeaderExtractorConfig();
    config.key = null;
    config.lineNumber = 3;
    
    config.regex = "(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)";
    headerExtractorConfigs.add(config);
    
	ProcessorRunner runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, null)
        .addConfiguration("fieldPathToParse", "/value")
        .addConfiguration("dataFormat", DataFormatType.AS_BLOB)
        .addConfiguration("keepOriginalFields", false)
        .addConfiguration("headerExtractorConfigs", headerExtractorConfigs)
        .addConfiguration("headerDetailSeparator", "^-----")
        .addConfiguration("nofHeaderLines", 15)
        .addConfiguration("outputField", "/")
        .addConfiguration("detailLineField", "/detail")
        .addConfiguration("splitDetails", false)
        .setExecutionMode(ExecutionMode.STANDALONE)
        .setResourcesDir("/tmp")
        .addOutputLane("output")
        .build();

    runner.runInit();

    try {
      Record r0 = createRecordWithValueAndTemplate(value);
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);

      List<Record> op = output.getRecords().get("output");
      
      System.out.println(op.get(0).get("/detail").getValueAsString());
      
      assertEquals(16 , op.size());
      assertEquals("11:02:12.000", StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));

    } finally {
      runner.runDestroy();
    }
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testWithEmptyFile() throws StageException, IOException {
	InputStream stream = null;
	
	stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("ROW01_N02_01B0B628_RB_20Hz_20161010_110212_empty.txt");
	String value = IOUtils.toString(stream);

	File dir = new File("target", UUID.randomUUID().toString());
    dir.mkdirs();
    Configuration.setFileRefsBaseDir(dir);

	
	//UtahParserDProcessor processor = new UtahParserDProcessor();
    
    List<HeaderExtractorConfig> headerExtractorConfigs = new ArrayList<>();
    HeaderExtractorConfig config = new HeaderExtractorConfig();
    config.key = null;
    config.lineNumber = 3;
    
    config.regex = "(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)";
    headerExtractorConfigs.add(config);
    
	ProcessorRunner runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, null)
        .addConfiguration("fieldPathToParse", "/value")
        .addConfiguration("dataFormat", DataFormatType.AS_BLOB)
        .addConfiguration("keepOriginalFields", false)
        .addConfiguration("headerExtractorConfigs", headerExtractorConfigs)
        .addConfiguration("headerDetailSeparator", "^-----")
        .addConfiguration("nofHeaderLines", 15)
        .addConfiguration("outputField", "/")
        .addConfiguration("detailLineField", "/detail")
        .addConfiguration("splitDetails", false)
        .setExecutionMode(ExecutionMode.STANDALONE)
        .setResourcesDir("/tmp")
        .addOutputLane("output")
        .build();

    runner.runInit();

    try {
      Record r0 = createRecordWithValueAndTemplate(value);
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);

      List<Record> op = output.getRecords().get("output");
      
      System.out.println(op.get(0).get("/detail").getValueAsString());
      
      assertEquals(16 , op.size());
      assertEquals("11:02:12.000", StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));

    } finally {
      runner.runDestroy();
    }
  }
  
  
  @Test
  @SuppressWarnings("unchecked")
  public void testLineSplit() throws StageException, IOException {
	InputStream stream = null;
	
	stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("ROW01_N02_01B0B628_RB_20Hz_20161010_110212_small.txt");
	String value = IOUtils.toString(stream);

	File dir = new File("target", UUID.randomUUID().toString());
    dir.mkdirs();
    Configuration.setFileRefsBaseDir(dir);

	
	//UtahParserDProcessor processor = new UtahParserDProcessor();
    
    List<HeaderExtractorConfig> headerExtractorConfigs = new ArrayList<>();
    HeaderExtractorConfig config = new HeaderExtractorConfig();
    config.key = null;
    config.lineNumber = 3;
    
    config.regex = "(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)";
    headerExtractorConfigs.add(config);
    
	ProcessorRunner runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, null)
        .addConfiguration("fieldPathToParse", "/value")
        .addConfiguration("dataFormat", DataFormatType.AS_BLOB)
        .addConfiguration("keepOriginalFields", false)
        .addConfiguration("headerExtractorConfigs", headerExtractorConfigs)
        .addConfiguration("headerDetailSeparator", "^-----")
//        .addConfiguration("nofHeaderLines", 15)
        .addConfiguration("outputField", "/")
        .addConfiguration("detailLineField", "/detail")
        .addConfiguration("splitDetails", true)
        .addConfiguration("separator", ",")
        .addConfiguration("headerType", HeaderType.USE_HEADER)
        .setExecutionMode(ExecutionMode.STANDALONE)
        .setResourcesDir("/tmp")
        .addOutputLane("output")
        .build();

    runner.runInit();

    try {
      Record r0 = createRecordWithValueAndTemplate(value);
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);

      List<Record> op = output.getRecords().get("output");
      
      System.out.println(op.get(0).get("/detail").getValueAsListMap());
      
      assertEquals(16 , op.size());
//      assertEquals("11:02:12.000", StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));

    } finally {
      runner.runDestroy();
    }
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testLineSplit2() throws StageException, IOException {
	InputStream stream = null;
	
	stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("ROW01_N02_01B0B628_RB_20Hz_20161010_110212_small.txt");
	String value = IOUtils.toString(stream);

	File dir = new File("target", UUID.randomUUID().toString());
    dir.mkdirs();
    Configuration.setFileRefsBaseDir(dir);

	
	//UtahParserDProcessor processor = new UtahParserDProcessor();
    
    List<HeaderExtractorConfig> headerExtractorConfigs = new ArrayList<>();
    HeaderExtractorConfig config = new HeaderExtractorConfig();
    config.key = null;
    config.lineNumber = 3;
    
    config.regex = "(\\w*)[:=, ]*(\"[^\"]*\"|[^\\s]*)";
    headerExtractorConfigs.add(config);
    
	ProcessorRunner runner = new ProcessorRunner.Builder(HeaderDetailParserDProcessor.class, null)
        .addConfiguration("fieldPathToParse", "/value")
        .addConfiguration("dataFormat", DataFormatType.AS_BLOB)
        .addConfiguration("keepOriginalFields", false)
        .addConfiguration("headerExtractorConfigs", headerExtractorConfigs)
        .addConfiguration("headerDetailSeparator", "^-----")
//        .addConfiguration("nofHeaderLines", 15)
        .addConfiguration("outputField", "/")
        .addConfiguration("detailLineField", "/")
        .addConfiguration("splitDetails", true)
        .addConfiguration("separator", ",")
        .addConfiguration("headerType", HeaderType.USE_HEADER)
        .setExecutionMode(ExecutionMode.STANDALONE)
        .setResourcesDir("/tmp")
        .addOutputLane("output")
        .build();

    runner.runInit();

    try {
      Record r0 = createRecordWithValueAndTemplate(value);
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);

      List<Record> op = output.getRecords().get("output");
      
      System.out.println(op.get(0));
      
      assertEquals(16 , op.size());
//      assertEquals("11:02:12.000", StringUtils.substring(op.get(0).get("/detail").getValueAsString(), 0, 12));

    } finally {
      runner.runDestroy();
    }
  }  
}
