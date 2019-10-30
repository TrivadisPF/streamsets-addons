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

import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.trivadis.streamsets.aws.stage.lib.s3.S3ConfigBean;
import com.trivadis.streamsets.aws.stage.processor.s3lookup.config.AmazonS3Util;
import com.trivadis.streamsets.aws.stage.processor.s3lookup.config.AwsS3LookupProcessorConfig;
import com.trivadis.streamsets.aws.stage.processor.s3lookup.config.DataFormatType;
import com.trivadis.streamsets.aws.stage.processor.s3lookup.config.S3FileRef;
import com.trivadis.streamsets.aws.util.AWSUtil;

/**
 * This executor is an example and does not actually perform any actions.
 */
public abstract class AwsS3LookupProcessor extends SingleLaneRecordProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(AwsS3LookupProcessor.class);

	/**
	 * Gives access to the UI configuration of the stage provided by the
	 * {@link SampleDProcessor} class.
	 */
	public abstract AwsS3LookupProcessorConfig getConfig();

//	private ErrorRecordHandler errorRecordHandler;
	private Map<String, ELEval> evals;

	@Override
	protected List<ConfigIssue> init() {
		List<ConfigIssue> issues = super.init();

		// errorRecordHandler = new DefaultErrorRecordHandler(getContext());

		getConfig().s3ConfigBean.init(getContext(), issues);

		return issues;

	}

	@Override
	public void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {

		AmazonS3 s3Client;
		S3ConfigBean s3ConfigBean = getConfig().s3ConfigBean;
		s3Client = s3ConfigBean.s3Config.getS3Client();
		
		String objectPath = (getConfig().objectPathFromField) ?
				record.get(getConfig().fieldWithObjectPath).getValueAsString() :
				getConfig().objectPath;
		objectPath = AWSUtil.normalizePath(objectPath, getConfig().s3ConfigBean.s3Config.delimiter);

		LOG.info((s3Client != null) ? "T" : "F");
		LOG.info(s3Client.doesBucketExist(s3ConfigBean.s3Config.bucket) ? "t" : "f");
		LOG.info("bucketName: " + s3ConfigBean.s3Config.bucket);
		LOG.info("objectPath: " + objectPath);

		S3Object object = AmazonS3Util.getObject(s3Client, s3ConfigBean.s3Config.bucket, objectPath,
				s3ConfigBean.sseConfig.useCustomerSSEKey, s3ConfigBean.sseConfig.customerKey,
				s3ConfigBean.sseConfig.customerKeyMd5);

		try {
			if (getConfig().dataFormat.equals(DataFormatType.AS_RECORDS)) {
				S3ObjectInputStream input = object.getObjectContent();
				InputStreamReader inr = new InputStreamReader(input, "UTF-8");
				String utf8str = IOUtils.toString(inr);

				for (String line : IOUtils.readLines(new StringReader(utf8str))) {
					record.set(getConfig().outputField, Field.create(line));
					batchMaker.addRecord(record);
				}
			} else if (getConfig().dataFormat.equals(DataFormatType.AS_BLOB)) {
				S3ObjectInputStream input = object.getObjectContent();
				InputStreamReader inr = new InputStreamReader(input, "UTF-8");
				String utf8str = IOUtils.toString(inr);

				record.set(getConfig().outputField, Field.create(utf8str));
				batchMaker.addRecord(record);
			} else if (getConfig().dataFormat.equals(DataFormatType.AS_WHOLE_FILE)) {
				HashMap<String, Field> root = new HashMap<>();
				
				ObjectMetadata metadata = object.getObjectMetadata();

				S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
				s3ObjectSummary.setBucketName(s3ConfigBean.s3Config.bucket);
				s3ObjectSummary.setKey(objectPath);
				s3ObjectSummary.setSize(metadata.getContentLength());
				s3ObjectSummary.setETag(metadata.getETag());
				s3ObjectSummary.setLastModified(metadata.getLastModified());
				s3ObjectSummary.setStorageClass(metadata.getStorageClass());
				
				record.set("/fileRef", Field.create(new S3FileRef.Builder().s3Client(s3Client)
									.s3ObjectSummary(s3ObjectSummary)
									.useSSE(s3ConfigBean.sseConfig.useCustomerSSEKey)
									.customerKey(s3ConfigBean.sseConfig.customerKey)
									.customerKeyMd5(s3ConfigBean.sseConfig.customerKeyMd5)
//                        									.bufferSize((int) dataParser.suggestedWholeFileBufferSize())
									.createMetrics(true)
//                        									.totalSizeInBytes(s3ObjectSummary.getSize())
//                        									.rateLimit(dataParser.wholeFileRateLimit())
									.build()));

				HashMap<String, Field> fileInfo = new HashMap<>();
				fileInfo.put("size", Field.create(metadata.getContentLength()));
				fileInfo.put("filename", Field.create(getConfig().objectPath));
				record.set("/fileInfo", Field.create(fileInfo));
				batchMaker.addRecord(record);
			} else {

			}
		} catch (OnRecordErrorException e) {
			throw new RuntimeException(e);
		} catch (Exception e) {
			LOG.error("Can't execute WASB operation", e);
			throw new RuntimeException(e);
		}

		LOG.info(object.getBucketName());
		LOG.info(object.getObjectMetadata().toString());

	}

}
