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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.RecordProcessor;
import com.trivadis.streamsets.stage.lib.headerdetailparser.Errors;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.DataFormatType;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.DetailsColumnHeaderType;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserDetailsConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.HeaderDetailParserHeaderConfig;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.OnStagePreConditionFailure;
import com.trivadis.streamsets.stage.processor.headerdetailparser.config.TooManySplitsAction;


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
	 
	private String[] fieldPathFromConfiguration;
	
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
	
	private String[] split(String text, String separator, int limit) {
		String newSeparator = separator.replace("\\t", "\t");
		String[] result;
		if (getDetailsConfig().separatorAsRegex) {
			result = text.split(separator, limit);
		} else {
			if (limit == 0) {
				result = (newSeparator.length() > 1) ? StringSplitter.fastSplit(text, newSeparator) : StringSplitter.fastSplit(text, newSeparator.charAt(0));
			} else {
				result = text.split(newSeparator, limit);
			}
		}
		return result;
	}

	  
	@Override
	protected List<ConfigIssue> init() {
		List<ConfigIssue> issues = super.init();
		
	    int count = 1;
	    if (getDetailsConfig().fieldPathsForSplits != null) {
	    	for(String fieldPath: getDetailsConfig().fieldPathsForSplits) {
	    		if(fieldPath == null || fieldPath.trim().length() == 0) {
	    			issues.add(getContext().createConfigIssue(Groups.DETAILS.name(), "fieldPathsForSplits",
	    					Errors.HEADERDETAILP_03, count));
	    		}
	    		count++;
	    	}
	    	if (getDetailsConfig().fieldPathsForSplits.size() == 0 && getDetailsConfig().tooManySplitsAction == TooManySplitsAction.TO_LAST_FIELD) {
	    		issues.add(getContext().createConfigIssue(Groups.DETAILS.name(), "fieldPathsForSplits",
	    				Errors.HEADERDETAILP_00));
	    	}
	    }
	    
		// compile all regex patterns
		if (getHeaderConfig() != null && getHeaderConfig().headerExtractorConfigs != null) {
			for (HeaderExtractorConfig headerExtractorConfig : getHeaderConfig().headerExtractorConfigs) {
				patterns.put(headerExtractorConfig.regex, Pattern.compile(headerExtractorConfig.regex));
			}
		}
	    if (issues.isEmpty()) {
	    	if (getDetailsConfig().fieldPathsForSplits != null) {
	    		fieldPathFromConfiguration = getDetailsConfig().fieldPathsForSplits.toArray(new String[getDetailsConfig().fieldPathsForSplits.size()]);
	    	}
	    }
	    
	    headerLane = getContext().getOutputLanes().get(0);
	    headerAndDetailLane = getContext().getOutputLanes().get(1);

		return issues;
	}

	/** {@inheritDoc} */
	@Override
	protected void process(Record record, BatchMaker batchMaker) throws StageException {
		// start clock for benchmark
		long startTime = System.currentTimeMillis();
		
	    if (getParserConfig().keepOriginalFields && !record.get().getType().isOneOf(Field.Type.MAP, Field.Type.LIST_MAP)) {
	        String errorValue;
	        if (record.get().getType() == Field.Type.LIST) {
	          errorValue = record.get().getValueAsList().toString();
	        } else {
	          errorValue = record.get().toString();
	        }
	        throw new OnRecordErrorException(
	            Errors.HEADERDETAILP_01, record.get().getType().toString(),
	            errorValue, record.toString());
	    }
	
	    Field original = null;
	    InputStream is = null;
	    if (getParserConfig().inputDataFormat.equals(DataFormatType.AS_BLOB)) {
			original = record.get(getParserConfig().fieldPathToParse);
			
			is = IOUtils.toInputStream(original.getValueAsString(), Charset.forName("UTF-8"));
	    	try {
	    		processText(is, record, batchMaker);
			} catch (IOException e) {
				throw new OnRecordErrorException(
			                Errors.HEADERDETAILP_08, record.getHeader().getSourceId(), e.getMessage());
			}
			
	    } else if (getParserConfig().inputDataFormat.equals(DataFormatType.AS_WHOLE_FILE)) {
	    	String fileName = record.get("/fileInfo/filename").getValueAsString();
			record.getHeader().setAttribute("/fileInfo/filename", fileName);
			
	    	FileRef fileRef = record.get("/fileRef").getValueAsFileRef();

	    	try {
	    		// get an input stream to process
				is = fileRef.createInputStream(getContext(), InputStream.class);
		        processText(is, record, batchMaker);
			} catch (IOException e) {
				throw new OnRecordErrorException(
			                Errors.HEADERDETAILP_04, record.getHeader().getSourceId(), e.getMessage());
			}
	    } else if (getParserConfig().inputDataFormat.equals(DataFormatType.AS_RECORDS)) {
	    	original = record.get(getParserConfig().fieldPathToParse);
	    	is = IOUtils.toInputStream(original.getValueAsString(), Charset.forName("UTF-8"));
	    	try {
	    		processText(is, record, batchMaker);
			} catch (IOException e) {
				throw new OnRecordErrorException(
			                Errors.HEADERDETAILP_09, record.getHeader().getSourceId(), e.getMessage());
			}
	    }
	    
	    LOG.info("HeaderDetailParser Time: " + (System.currentTimeMillis()-startTime) + "ms"); 
	}
	
	private void processText(InputStream is, Record record, BatchMaker batchMaker) throws StageException, IOException {
		List<String> headers = new ArrayList<>();
        String line = null;
       
        Pattern headerDetailSeparatorRegex = null; 
        if (getHeaderConfig().headerDetailSeparator != null && getHeaderConfig().headerDetailSeparator.length() > 0) {
        	headerDetailSeparatorRegex = Pattern.compile(getHeaderConfig().headerDetailSeparator);   
        }
        
        if (!getParserConfig().keepOriginalFields) {
        	record.delete(getParserConfig().fieldPathToParse);
        	record.delete("/fileRef");
        }
        
        // get the largest value of headerExtractorConfig.headerConfig.lineNumber and the headerConfig.nofHeaderLines
		int nofHeaderLines = (getHeaderConfig().nofHeaderLines !=null) ? getHeaderConfig().nofHeaderLines : 0;
		if (headerDetailSeparatorRegex == null) {
			for (HeaderExtractorConfig headerExtractorConfig : getHeaderConfig().headerExtractorConfigs) {
				nofHeaderLines = Integer.max(nofHeaderLines, headerExtractorConfig.lineNumber);
			}
		}

		BufferedReader br = new BufferedReader(new InputStreamReader(is));

		boolean withinHeader = true;

		int idx = 1;
		while (withinHeader && (line = br.readLine()) != null) {
			if (withinHeader) {
				if (headerDetailSeparatorRegex != null && headerDetailSeparatorRegex.matcher(line).find()) {
					line = br.readLine();
					withinHeader = false;
				} else if (getHeaderConfig().nofHeaderLines != null && idx > nofHeaderLines) {
					withinHeader = false;
				} else {
					headers.add(line);
				}
			} 
			idx++;
		}
		
		if (idx==1) {
			throw new OnRecordErrorException(Errors.HEADERDETAILP_06, record.getHeader().getSourceId());
		}
		
		if (headers.size() > 0) {
			for (HeaderExtractorConfig headerExtractorConfig : getHeaderConfig().headerExtractorConfigs) {
	
				String header = headers.get(headerExtractorConfig.lineNumber-1);
				Matcher matcher = getPattern(patterns, headerExtractorConfig.regex).matcher(header);
				if (matcher.find()) {
					LOG.debug("matcher found");
					String key = headerExtractorConfig.key;
					// take the value from group 1, if no key is specified
					if (headerExtractorConfig.key == null || headerExtractorConfig.key.length() == 0) {
						key = matcher.group(1);
					}
					LOG.debug("key: " + key);
					String val = matcher.group(2);
					LOG.debug("val: " + val);
					record.set("/" + key, Field.create(val));
				} else {
					LOG.debug("no matcher found: " + matcher);
				}
			}
			// only add an output to the headerLane, if there is at least one regex defined
			if (getHeaderConfig().headerExtractorConfigs.size() > 0) {
				batchMaker.addRecord(record, headerLane);
			}
		}

		String detailHeader = null;
		boolean parseDetailHeader = true;

		// if there is an overlap of header lines and details
		if (getHeaderConfig().nofHeaderLines != null && getHeaderConfig().nofHeaderLines != headers.size()) {
			// loop through detail lines which were also part of header and process them
			for (int i = getHeaderConfig().nofHeaderLines; i < headers.size(); i++) {
				if (!getDetailsConfig().detailsColumnHeaderType.equals(DetailsColumnHeaderType.NO_HEADER) && parseDetailHeader) {
					detailHeader = line;
					parseDetailHeader = false;
					detailHeader = headers.get(i);
				} else {
					processDetailLine(record, headers.get(i), detailHeader);
					batchMaker.addRecord(record, headerAndDetailLane);
				}
			}
		}
		
		// loop through detail lines not part of header and process them
		while (line != null && line.length() > 0) {
			if (!getDetailsConfig().detailsColumnHeaderType.equals(DetailsColumnHeaderType.NO_HEADER) && parseDetailHeader) {
				detailHeader = line;
				parseDetailHeader = false;
			} else {
				processDetailLine(record, line, detailHeader);
				batchMaker.addRecord(record, headerAndDetailLane);
			}
			line = br.readLine();
		}
		br.close();
	}
		
	
	public Field processDetailLine(Record record, String line, String detailHeader) throws StageException {
		Field field = null;
		ErrorCode error = null;
		 
		if (line.isEmpty()) {
			throw new OnRecordErrorException(Errors.HEADERDETAILP_05, record.getHeader().getSourceId());			
		}
		
		if (getParserConfig().splitDetails) {
			String[] splits = null;
			String[] fieldPaths = fieldPathFromConfiguration;
			LinkedHashMap<String, Field> listMap = new LinkedHashMap<>();
			
			if (getDetailsConfig().detailsColumnHeaderType.equals(DetailsColumnHeaderType.USE_HEADER)) {
				fieldPaths = split(detailHeader, getDetailsConfig().separator, 0);
				//fieldPaths = detailHeader.split(getDetailsConfig().separator);
				for (int i = 0; i < fieldPaths.length; i++) {
					// get the field name from the header, removing double quotes and single quotes from the text 
					fieldPaths[i] = "/" + StringUtils.remove(StringUtils.remove(fieldPaths[i], "\""), "'");
				}
			}

			/**
			 * the following code is copied from the SplitterProcessor. Except the split method which
			 * is implemented with more functionality than the String.split class used in the SplitterProcessor.
			 */
			switch (getDetailsConfig().tooManySplitsAction) {
		        case TO_LAST_FIELD:
		          splits = split(line, getDetailsConfig().separator, fieldPaths.length);
		          break;
		        case TO_LIST:
		          splits = split(line, getDetailsConfig().separator, 0);
		          break;
		        default:
		          throw new IllegalArgumentException("Unsupported value for too many splits action: " + getDetailsConfig().tooManySplitsAction);
			}
			
	        if (splits.length < fieldPaths.length) {
	        	error = Errors.HEADERDETAILP_02;
//	        	throw new OnRecordErrorException(Errors.HEADERDETAILP_02, record.getHeader().getSourceId());
	        }
	        
	        if (error == null || getDetailsConfig().onStagePreConditionFailure == OnStagePreConditionFailure.CONTINUE) {
		        int i = 0;
		        for (i = 0; i < fieldPaths.length; i++) {
		          try {
		            if (splits != null && splits.length > i) {
		              listMap.put(fieldPaths[i], Field.create(splits[i]));
		            } else {
		              listMap.put(fieldPaths[i], Field.create(Field.Type.STRING, null));
		            }
		          } catch (IllegalArgumentException e) {
		            throw new OnRecordErrorException(Errors.HEADERDETAILP_07, fieldPaths[i], record.getHeader().getSourceId(),
		              e.toString());
		          }
		        }
	
		        if (splits != null && i < splits.length && getDetailsConfig().tooManySplitsAction == TooManySplitsAction.TO_LIST) {
		          List<Field> remainingSplits = Lists.newArrayList();
		          for (int j = i; j < splits.length; j++) {
		            remainingSplits.add(Field.create(splits[j]));
		          }
		          listMap.put(getDetailsConfig().remainingSplitsPath, Field.create(remainingSplits));
		        }
		        
	        } else {
	            throw new OnRecordErrorException(error, record.getHeader().getSourceId(), getDetailsConfig().fieldPathsForSplits);
	        }
	        // ===> copied till here
	        
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
