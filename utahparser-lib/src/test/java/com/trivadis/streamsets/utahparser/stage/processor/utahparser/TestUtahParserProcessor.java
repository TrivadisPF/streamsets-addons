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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

import _ss_com.com.google.common.collect.ImmutableList;
import _ss_com.streamsets.datacollector.util.Configuration;
import _ss_org.apache.commons.io.IOUtils;
import junit.framework.Assert;

public class TestUtahParserProcessor {
	
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
	
	stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("cisco_bgp_summary_example.txt");
	String value = IOUtils.toString(stream);

	stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("cisco_bgp_summary_template.xml");
	String template = IOUtils.toString(stream);


	File dir = new File("target", UUID.randomUUID().toString());
    dir.mkdirs();
    Configuration.setFileRefsBaseDir(dir);

	
	//UtahParserDProcessor processor = new UtahParserDProcessor();
	
	ProcessorRunner runner = new ProcessorRunner.Builder(UtahParserDProcessor.class, null)
        .addConfiguration("template", template)
        .addConfiguration("fieldPathToParse", "/value")
        .addConfiguration("parsedFieldPath", "/output")
        .setExecutionMode(ExecutionMode.STANDALONE)
        .setResourcesDir("/tmp")
        .addOutputLane("output")
        .build();

    runner.runInit();

    try {
      Record r0 = createRecordWithValueAndTemplate(value);
      List<Record> input = ImmutableList.of(r0);
      StageRunner.Output output = runner.runProcess(input);

    } finally {
      runner.runDestroy();
    }
  }
}
