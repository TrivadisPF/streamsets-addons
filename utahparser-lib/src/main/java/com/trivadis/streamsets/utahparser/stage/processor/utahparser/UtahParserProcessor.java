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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.commons.lang3.StringUtils;

import com.sonalake.utah.Parser;
import com.sonalake.utah.config.Config;
import com.sonalake.utah.config.ConfigLoader;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.trivadis.streamsets.utahparser.stage.lib.sample.Errors;

public abstract class UtahParserProcessor extends SingleLaneRecordProcessor {
	/**
	 * Gives access to the UI configuration of the stage provided by the
	 * {@link SampleDProcessor} class.
	 */
	public abstract String getFieldPathToParse();
	
	public abstract boolean isKeepOriginalFields();

	public abstract String getOutputField();

	public abstract String getTemplate();

	/** {@inheritDoc} */
	@Override
	protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {

		Field field = record.get(getFieldPathToParse());
		if (field == null) {
			throw new OnRecordErrorException(Errors.UTAHP_00, record.getHeader().getSourceId(), getFieldPathToParse());
		} else {

		}
		String value = field.getValueAsString();
		
		if (!isKeepOriginalFields()) {
			record.delete(getFieldPathToParse());
		}	
		
		Config config = null;
		try {
			config = new ConfigLoader().loadConfig(new StringReader(getTemplate()));
		} catch (JAXBException ex) {
			throw new OnRecordErrorException(Errors.UTAHP_01, record.getHeader().getSourceId(), ex.toString(), ex);
		}

		System.out.println("Template parsed successfully!");

		try (Reader in = new StringReader(value)) {
			Parser parser = Parser.parse(config, in);
			while (true) {
				Map<String, String> values = parser.next();
				if (null == values) {
					break;
				} else {
					LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();

					for (String key : values.keySet()) {
						String val = values.get(key);
						Field f = Field.create(val);
						listMap.put(key, f);
					}
					
					if (isKeepOriginalFields() && !StringUtils.isEmpty(getOutputField())) {
						record.set("/" + getOutputField(), Field.createListMap(listMap));
					} else {
						for(String key : listMap.keySet()) {
							record.set("/" + key, listMap.get(key));
						}
					}
					batchMaker.addRecord(record);
				}
			}
		} catch (IOException ex) {
			throw new OnRecordErrorException(Errors.UTAHP_01, record.getHeader().getSourceId(), ex.toString(), ex);
		}

	}
}
