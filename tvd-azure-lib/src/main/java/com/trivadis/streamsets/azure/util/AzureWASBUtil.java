package com.trivadis.streamsets.azure.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;


public class AzureWASBUtil {

	public static InputStream getObject(CloudBlobClient blobClient, String containerName, String objectPath, boolean useSSE) throws IOException {
		InputStream is = null;
		CloudBlobContainer container = null;
		CloudBlob blob = null;

		try {
			container = blobClient.getContainerReference(containerName);
			container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
					new OperationContext());
			blob = container.getBlockBlobReference(StringUtils.removeStart(objectPath, "/"));
			is = blob.openInputStream();
		} catch (StorageException e) {
			throw new IOException(e);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		return is;
		
	}
	
	public static Map<String,String> getMetaData(CloudBlobClient blobClient, String containerName, String objectPath, boolean useSSE) throws IOException {
		Map<String, String> metadata = null;
		CloudBlobContainer container = null;
		CloudBlob blob = null;

		try {
			container = blobClient.getContainerReference(containerName);
			container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
					new OperationContext());
			blob = container.getBlockBlobReference(StringUtils.removeStart(objectPath, "/"));
			metadata = blob.getMetadata();
		} catch (StorageException e) {
			throw new IOException(e);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		return metadata;
	}
	
}
