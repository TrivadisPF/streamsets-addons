package com.trivadis.streamsets.azure.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
//import com.streamsets.pipeline.api.StageException;
//import com.streamsets.pipeline.api.credential.CredentialValue;


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
	
	public static BlobProperties getBlobProperties(CloudBlobClient blobClient, String containerName, String objectPath, boolean useSSE) throws IOException {
		BlobProperties properties = null;
		CloudBlobContainer container = null;
		CloudBlob blob = null;

		try {
			container = blobClient.getContainerReference(containerName);
			container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
					new OperationContext());
			blob = container.getBlockBlobReference(StringUtils.removeStart(objectPath, "/"));
			blob.downloadAttributes();
			properties = blob.getProperties();
		} catch (StorageException e) {
			throw new IOException(e);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		return properties;
		
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
			blob.downloadAttributes();
			metadata = blob.getMetadata();
		} catch (StorageException e) {
			throw new IOException(e);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
		return metadata;
	}
	
	
	private final static String TEST_CONTAINER = "gus-test";
	private final static String TEST_OBJECT_PATH = "big-test-wide.csv";
	//private final static String TEST_OBJECT_PATH = "orsted/2018-04-20-21/ROW01_N02_01B0B628_RB_20Hz_20161010_110212-11.txt.ready";
	
	public static void main(String[] args) throws IOException {
		CloudBlobClient blobClient = null;

		String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName="
				+ "dorstrefinedatastac001" + ";" + "AccountKey="
				+ "wxUsBLaNXzgJFV4YkNyfHnVKrjB5sxnbfYIFuqc1Y43gIqx/o+2qXHDoFuQuH7BGhXFZMjfoiDiNkNSO+gDVsQ==";
		
		CloudStorageAccount storageAccount = null;

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
		
		long startTime = System.currentTimeMillis();

		InputStream is = AzureWASBUtil.getObject(blobClient, TEST_CONTAINER, TEST_OBJECT_PATH, true);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line;
		while (true) {
			line = br.readLine();
			if (line == null) {
				break;
			}
		}
		System.out.println("testGetObject - Total execution time: " + (System.currentTimeMillis()-startTime) + "ms"); 


	}
	
}
