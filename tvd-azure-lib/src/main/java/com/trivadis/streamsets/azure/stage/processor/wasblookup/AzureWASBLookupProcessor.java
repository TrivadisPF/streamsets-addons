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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringBufferInputStream;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseExecutor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.trivadis.streamsets.azure.stage.processor.wasblookup.config.AzureWASBLookupProcessorConfig;
import com.trivadis.streamsets.azure.stage.processor.wasblookup.config.OutputModeType;

/**
 * This executor is an example and does not actually perform any actions.
 */
public abstract class AzureWASBLookupProcessor extends SingleLaneRecordProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(AzureWASBLookupProcessor.class);

	private CloudStorageAccount storageAccount = null;
	private CloudBlobClient blobClient = null;

	/**
	 * Gives access to the UI configuration of the stage provided by the
	 * {@link SampleDProcessor} class.
	 */
	public abstract AzureWASBLookupProcessorConfig getConfig();

	// private ErrorRecordHandler errorRecordHandler;
	private Map<String, ELEval> evals;

	@Override
	protected List<ConfigIssue> init() {
		List<ConfigIssue> issues = super.init();

		return issues;
	}

	@Override
	public void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
		String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName="
				+ getConfig().azureConfig.accountName.get() + ";" + "AccountKey="
				+ getConfig().azureConfig.accountKey.get();

		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
		} catch (InvalidKeyException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (URISyntaxException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		blobClient = storageAccount.createCloudBlobClient();

		ELVars variables = getContext().createELVars();

		try {
			// Calculate working file (the same for all task types)
			String containerName = (getConfig().containerFromField) ? 
										record.get(getConfig().fieldWithContainer).getValueAsString() :
										getConfig().container;				
System.out.println(record.get(getConfig().fieldWithObjectPath).getValueAsString());
System.out.println(getConfig().objectPath);

			String objectPath = (getConfig().objectPathFromField) ?
									record.get(getConfig().fieldWithObjectPath).getValueAsString() :
									getConfig().objectPath;

			if (containerName == null || containerName.isEmpty()) {
				throw new OnRecordErrorException(record, Errors.WASB_EXECUTOR_0003);
			}
			if (objectPath == null || objectPath.isEmpty()) {
				throw new OnRecordErrorException(record, Errors.WASB_EXECUTOR_0004);
			}
			LOG.debug("Working on {}:{}", containerName, objectPath);

			// download the object into the value field
			CloudBlob blob = getBlob(containerName, objectPath);

			InputStream input = blob.openInputStream();
			InputStreamReader inr = new InputStreamReader(input, "UTF-8");
			String utf8str = IOUtils.toString(inr);

			if (getConfig().outputMode.equals(OutputModeType.AS_RECORDS)) {
				for (String line : IOUtils.readLines(new StringReader(utf8str))) {
					record.set("/value", Field.create(line));
					batchMaker.addRecord(record);
				}
			} else {
				record.set("/value", Field.create(utf8str));
				batchMaker.addRecord(record);
			}

		} catch (OnRecordErrorException e) {
			// errorRecordHandler.onError(e);
		} catch (Exception e) {
			LOG.error("Can't execute WASB operation", e);
			// errorRecordHandler.onError(new OnRecordErrorException(record,
			// Errors.WASB_EXECUTOR_0000, e.toString()));
		}
	}

	private CloudBlob getBlob(String containerName, String objectPath) throws OnRecordErrorException {
		CloudBlob blob = null;
		CloudBlobContainer container = null;

		try {
			container = blobClient.getContainerReference(containerName);
			container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
					new OperationContext());
			blob = container.getBlockBlobReference(objectPath);
		} catch (URISyntaxException | StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Getting a blob reference
		return blob;

	}


}
