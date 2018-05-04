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
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.trivadis.streamsets.azure.util.AzureWASBUtil;

public class TestAzureWASBUtil {
	private final static String TEST_CONTAINER = "raw-data";
	private final static String TEST_OBJECT_PATH = "orsted/2018-05-03-15/ROW01_N02_01B0B628_RB_20Hz_20161010_110212_small21.txt.ready";
	//private final static String TEST_OBJECT_PATH = "orsted/2018-04-20-21/ROW01_N02_01B0B628_RB_20Hz_20161010_110212-11.txt.ready";
	private final static CredentialValue STORAGE_ACCOUNT_NAME = new CredentialValue() {
		@Override
		public String get() throws StageException {
			return "porststac001";
		}
	};
	private final static CredentialValue STORAGE_ACCOUNT_ACCESS_KEY = new CredentialValue() {
		@Override
		public String get() throws StageException {
			return "KlfnPZyUAKdmll/h+XCUISX6mhGtqZpASt/OwqPKNEj1jz6R1Ntxu0DpEl+SdtFoLlJLKalBhR5td6n8W2jElg==";
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
		InputStream is = AzureWASBUtil.getObject(blobClient, TEST_CONTAINER, TEST_OBJECT_PATH, true);
		InputStreamReader inr = new InputStreamReader(is, "UTF-8");
		String utf8str = IOUtils.toString(inr);

		assertNotNull(utf8str);
		assertEquals(19728, utf8str.length());
	}

	@Test
	public void testGetObjectWithLeadingSlashInObjectName() throws IOException {
		InputStream is = AzureWASBUtil.getObject(blobClient, TEST_CONTAINER, "/" + TEST_OBJECT_PATH, true);
		InputStreamReader inr = new InputStreamReader(is, "UTF-8");
		String utf8str = IOUtils.toString(inr);

		assertNotNull(utf8str);
		assertEquals(19728, utf8str.length());
	}
	
	@Test
	public void testGetMetadata() throws IOException {
		Map<String, String> metadata = AzureWASBUtil.getMetaData(blobClient, TEST_CONTAINER, "/" + TEST_OBJECT_PATH, true);
		System.out.println(metadata);
	}

}
