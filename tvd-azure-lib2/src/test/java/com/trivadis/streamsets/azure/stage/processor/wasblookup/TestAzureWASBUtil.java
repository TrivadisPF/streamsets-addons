package com.trivadis.streamsets.azure.stage.processor.wasblookup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.trivadis.streamsets.azure.util.AzureWASBUtil;

public class TestAzureWASBUtil {
	private final static String TEST_CONTAINER = "gus-test";
	private final static String TEST_OBJECT_PATH = "big-test-wide.csv";
	//private final static String TEST_OBJECT_PATH = "orsted/2018-04-20-21/ROW01_N02_01B0B628_RB_20Hz_20161010_110212-11.txt.ready";
	private final static CredentialValue STORAGE_ACCOUNT_NAME = new CredentialValue() {
		@Override
		public String get() throws StageException {
			return "dorstrefinedatastac001";
		}
	};
	private final static CredentialValue STORAGE_ACCOUNT_ACCESS_KEY = new CredentialValue() {
		@Override
		public String get() throws StageException {
			return "wxUsBLaNXzgJFV4YkNyfHnVKrjB5sxnbfYIFuqc1Y43gIqx/o+2qXHDoFuQuH7BGhXFZMjfoiDiNkNSO+gDVsQ==";
		}
	};
	
	
	private CloudBlobClient blobClient = null;
	
	@Before
	public void setup() throws StageException {
		String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName="
				+ STORAGE_ACCOUNT_NAME.get() + ";" + "AccountKey="
				+ STORAGE_ACCOUNT_ACCESS_KEY.get();
		
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
	}
	
	@Test
	public void testGetObject() throws IOException {
		long startTime = System.currentTimeMillis();

		InputStream is = AzureWASBUtil.getObject(blobClient, TEST_CONTAINER, TEST_OBJECT_PATH, true);
		InputStreamReader inr = new InputStreamReader(is, "UTF-8");
		String utf8str = IOUtils.toString(inr);

		System.out.println("testGetObject - Total execution time: " + (System.currentTimeMillis()-startTime) + "ms"); 

		assertNotNull(utf8str);
		assertEquals(169054181, utf8str.length());
	}

	@Test
	public void testGetObjectWithLeadingSlashInObjectName() throws IOException {
		InputStream is = AzureWASBUtil.getObject(blobClient, TEST_CONTAINER, "/" + TEST_OBJECT_PATH, true);
		InputStreamReader inr = new InputStreamReader(is, "UTF-8");
		String utf8str = IOUtils.toString(inr);

		assertNotNull(utf8str);
		assertEquals(169054181, utf8str.length());
	}
	
	@Test
	public void testGetMetadata() throws IOException {
		Map<String, String> metadata = AzureWASBUtil.getMetaData(blobClient, TEST_CONTAINER, "/" + TEST_OBJECT_PATH, true);
		System.out.println(metadata);
	}
	
	@Test
	public void testGetBlobProoperties() throws IOException {
		BlobProperties blob = AzureWASBUtil.getBlobProperties(blobClient, TEST_CONTAINER, "/" + TEST_OBJECT_PATH, true);
		System.out.println("lenght:" + blob.getLength());
		System.out.println(blob);
	}	

}
