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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.trivadis.streamsets.stage.lib.headerdetailparser.Errors;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.DataFormatType;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserDetailsConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserHeaderConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.DetailsColumnHeaderType;


public abstract class HeaderDetailParserProcessor extends RecordProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(HeaderDetailParserProcessor.class);
	
	/**
	 * Gives access to the UI configuration of the stage provided by the
	 * {@link SampleDProcessor} class.
	 */
	public abstract HeaderDetailParserConfig getParserConfig();
	public abstract HeaderDetailParserHeaderConfig getHeaderConfig();
	public abstract HeaderDetailParserDetailsConfig getDetailsConfig();
		
	// map of compiled regex, keyed by regex value
	private Map<String, Pattern> patterns = new HashMap<>();
	 
	private String[] fieldPaths;
	
	private String headerLane;
	private String headerAndDetailLane;
	
	private static Pattern getPattern(Map<String, Pattern> patterns, String regEx) {
		if (patterns != null && patterns.containsKey(regEx)) {
			return patterns.get(regEx);
		} else {
			Pattern pattern = Pattern.compile(regEx);
			if (patterns != null) {
				patterns.put(regEx, pattern);
			}
			return pattern;
		}
	}

	@Override
	protected List<ConfigIssue> init() {
		List<ConfigIssue> issues = super.init();
		
		// compile all patterns
		if (getHeaderConfig() != null && getHeaderConfig().headerExtractorConfigs != null) {
			for (HeaderExtractorConfig headerExtractorConfig : getHeaderConfig().headerExtractorConfigs) {
				patterns.put(headerExtractorConfig.regex, Pattern.compile(headerExtractorConfig.regex));
			}
		}
	    if (issues.isEmpty()) {
	    	if (getDetailsConfig().fieldPathsForSplits != null) {
	    		fieldPaths = getDetailsConfig().fieldPathsForSplits.toArray(new String[getDetailsConfig().fieldPathsForSplits.size()]);
	    	}
//	        removeUnsplitValue = originalFieldAction == OriginalFieldAction.REMOVE;
	    }
	    
	    headerLane = getContext().getOutputLanes().get(0);
	    headerAndDetailLane = getContext().getOutputLanes().get(1);

		return issues;
	}

	/** {@inheritDoc} */
	@Override
	protected void process(Record record, BatchMaker batchMaker) throws StageException {

		//record.getHeader().getSourceId()
		
	    if (getParserConfig().keepOriginalFields && !record.get().getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
	        String errorValue;
	        if (record.get().getType() == Field.Type.LIST) {
	          errorValue = record.get().getValueAsList().toString();
	        } else {
	          errorValue = record.get().toString();
	        }
	        throw new OnRecordErrorException(
	            Errors.HEADERDETAILP_02, record.get().getType().toString(),
	            errorValue, record.toString());
	    }
	
	    Field original = null;
	    InputStream is = null;
	    if (getParserConfig().inputDataFormat.equals(DataFormatType.AS_BLOB)) {
			original = record.get(getParserConfig().fieldPathToParse);
			
			is = IOUtils.toInputStream(original.getValueAsString(), Charset.forName("UTF-8"));
	        processOriginal(is, record, batchMaker);
			
	    } else if (getParserConfig().inputDataFormat.equals(DataFormatType.AS_WHOLE_FILE)) {
//	    	String fileName = record.get("/fileInfo/filename").getValueAsString();
	    	FileRef fileRef = record.get("/fileRef").getValueAsFileRef();
		    
			try {
				is = fileRef.createInputStream(getContext(), InputStream.class);
			    InputStreamReader inr = new InputStreamReader(is, "UTF-8");
				String utf8str = IOUtils.toString(inr);
				original = Field.create(utf8str);
				
				is = fileRef.createInputStream(getContext(), InputStream.class);
		        processOriginal(is, record, batchMaker);
			} catch (IOException e) {
				throw new OnRecordErrorException(
			                Errors.HEADERDETAILP_04, record.getHeader().getSourceId(), e.getMessage());
			}
	    } 

	}
	
	private void processOriginal(InputStream is, Record orinalRecord, BatchMaker batchMaker) throws StageException {
		List<String> headers = new ArrayList<>();
		String detailHeader = null;
        Record record = null;
        String line = null;
       
        Pattern headerDetailSeparatorRegex = null; 
        if (getHeaderConfig().headerDetailSeparator != null && getHeaderConfig().headerDetailSeparator.length() > 0) {
        	headerDetailSeparatorRegex = Pattern.compile(getHeaderConfig().headerDetailSeparator);   
        }
        
        if (getParserConfig().keepOriginalFields) {
        	record = getContext().cloneRecord(orinalRecord);
        } else {
        	record = getContext().createRecord(orinalRecord);
            record.set(Field.create(new HashMap<String, Field>()));
        }

        /*
        try {
			InputStreamReader inr = new InputStreamReader(is, "UTF-8");
			BufferedReader br = new BufferedReader(inr);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
        	
		Scanner scan = new Scanner(is);
//		scan.useDelimiter(StringUtils.LF);
		
		boolean withinHeader = true;
		boolean parseDetailHeader = true;

		int i = 1;
		while (scan.hasNextLine()) {
			line = scan.nextLine();

			if (withinHeader) {
				if (headerDetailSeparatorRegex != null && headerDetailSeparatorRegex.matcher(line).find()) {
					withinHeader = false;
				} else if (getHeaderConfig().nofHeaderLines != null && i >= getHeaderConfig().nofHeaderLines) {
					withinHeader = false;
				} else {
					headers.add(line);
				}
			} else {
				System.out.println("Number of header lines: " + headers.size());
				
				if (!getDetailsConfig().detailsColumnHeaderType.equals(DetailsColumnHeaderType.NO_HEADER) && parseDetailHeader) {
					detailHeader = line;
					parseDetailHeader = false;
					System.out.println("detail header: " + line);
				} else {
					break;
				}
			}
			i++;
		}
		
		if (i==1) {
			throw new OnRecordErrorException(Errors.HEADERDETAILP_03, record.getHeader().getSourceId());
		}
		
		if (headers.size() > 0) {
			for (HeaderExtractorConfig headerExtractorConfig : getHeaderConfig().headerExtractorConfigs) {
	
				String header = headers.get(headerExtractorConfig.lineNumber-1);
				System.out.println("header: " + header);
				System.out.println("regex: " + headerExtractorConfig.regex);
				Matcher matcher = getPattern(patterns, headerExtractorConfig.regex).matcher(header);
				if (matcher.find()) {
					LOG.info("matcher found");
					String key = headerExtractorConfig.key;
					// take the value from group 1, if no key is specified
					if (headerExtractorConfig.key == null || headerExtractorConfig.key.length() == 0) {
						key = matcher.group(1);
					}
					LOG.info("key: " + key);
					String val = matcher.group(2);
					LOG.info("val: " + val);
					record.set("/" + key, Field.create(val));
				} else {
					LOG.info("no matcher found: " + matcher);
				}
			}
			batchMaker.addRecord(record, headerLane);
		}
		
		while (line != null && line.length() > 0) {
			processDetail(record, line, detailHeader);
			batchMaker.addRecord(record, headerAndDetailLane);

			line = (scan.hasNextLine()) ? scan.nextLine() : null;
		}
	
	}
	
	public Field processDetail(Record record, String line, String detailHeader) throws StageException {
		Field field = null;
		
		if (line.isEmpty()) {
			throw new OnRecordErrorException(Errors.HEADERDETAILP_05, record.getHeader().getSourceId());			
		}
		
		if (getParserConfig().splitDetails) {
			String[] splits = null;
			String[] headers = fieldPaths;
			LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
			
			if (getDetailsConfig().detailsColumnHeaderType.equals(DetailsColumnHeaderType.USE_HEADER)) {
				headers = detailHeader.split(getDetailsConfig().separator);
				for (int i = 0; i < headers.length; i++) {
					headers[i] = "/" + headers[i];
				}
			}
	        splits = line.split(getDetailsConfig().separator);
			
	        /*
			switch (tooManySplitsAction) {
		        case TO_LAST_FIELD:
		          splits = line.split(getSeparator(), fieldPaths.length);
		          break;
		        case TO_LIST:
		          splits = line.split(getSeparator());
		          break;
		        default:
		          throw new IllegalArgumentException("Unsupported value for too many splits action: " + tooManySplitsAction);
			 }
	        if (splits.length < fieldPaths.length) {
	            error = Errors.SPLITTER_02;
	        }
			*/
	        
	        int i = 0;
	        for (i = 0; i < headers.length; i++) {
	          try {
	            if (splits != null && splits.length > i) {
	              listMap.put(headers[i], Field.create(splits[i]));
	            } else {
	              listMap.put(headers[i], Field.create(Field.Type.STRING, null));
	            }
	          } catch (IllegalArgumentException e) {
//	            throw new OnRecordErrorException(Errors.HEADERDETAILP_05, fieldPath, record.getHeader().getSourceId(),
//	              e.toString());
	          }
	        }

	        /*
	        if (splits != null && i < splits.length && tooManySplitsAction == TooManySplitsAction.TO_LIST) {
	          List<Field> remainingSplits = Lists.newArrayList();
	          for (int j = i; j < splits.length; j++) {
	            remainingSplits.add(Field.create(splits[j]));
	          }
	          listMap.put(remainingSplitsPath, Field.create(remainingSplits));
	        }	        
	        */
	        
	        if (getParserConfig().detailLineField != null && getParserConfig().detailLineField.length() > 0 && !getParserConfig().detailLineField.equals("/")) {
	        	record.set(getParserConfig().detailLineField,  Field.createListMap(listMap));
	        } else {
	        	for (String name : listMap.keySet()) {
	        		record.set(name, listMap.get(name));
	        	}
	        }
		} else {
			field = Field.create(line);
			record.set(getParserConfig().detailLineField, field);
		}
		return field;
	}
}
