package AmazonS3Util;

import java.io.InputStream;
import java.net.URISyntaxException;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class AzureWASBUtil {

	public static InputStream getObject(CloudBlobClient blobClient, String containerName, String objectPath, boolean useSSE) {
		InputStream is = null;
		CloudBlobContainer container = null;
		CloudBlob blob = null;
		
		try {
			container = blobClient.getContainerReference(containerName);
			container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
					new OperationContext());
			blob = container.getBlockBlobReference(objectPath);
			is = blob.openInputStream();
		} catch (URISyntaxException | StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return is;
		
	}
	
}
