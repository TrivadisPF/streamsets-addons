package com.trivadis.streamsets.azure.stage.processor.wasblookup.config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.Collections;
import java.util.Set;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.ProtoConfigurableEntity.Context;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;

import AmazonS3Util.AzureWASBUtil;

public class WASBFileRef extends FileRef {
	private final boolean useSSE;
	private final CredentialValue storageAccountName;
	private final CredentialValue storageAccountAccessKey;
	private final String containerName;
	private final String objectPath;

	private CloudStorageAccount storageAccount = null;
	private CloudBlobClient blobClient = null;

	@SuppressWarnings("unchecked")
	public WASBFileRef(CredentialValue storageAccountName, CredentialValue storageAccountAccessKey,
			String containerName, String objectPath, boolean useSSE) throws StageException {
		super(0);
		this.useSSE = useSSE;
		this.storageAccountName = storageAccountName;
		this.storageAccountAccessKey = storageAccountAccessKey;
		this.containerName = containerName;
		this.objectPath = objectPath;

		String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=" + storageAccountName.get()
				+ ";" + "AccountKey=" + storageAccountAccessKey.get();

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

	private final Set<Class<? extends AutoCloseable>> supportedStreamClasses = Collections.singleton(InputStream.class);

	@Override
	public <T extends AutoCloseable> Set<Class<T>> getSupportedStreamClasses() {
		return (Set) supportedStreamClasses;
	}

	@Override
	public <T extends AutoCloseable> T createInputStream(Context context, Class<T> aClass) throws IOException {
		return (T) AzureWASBUtil.getObject(blobClient, containerName, objectPath, useSSE);

//		return (T) new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8.name()));
	}

	public static final class Builder // extends AbstractFileRef.Builder<S3FileRef, Builder>
	{
		private boolean useSSE;
		private CredentialValue storageAccountName;
		private CredentialValue storageAccountAccessKey;
		private String containerName;
		private String objectPath;

		public Builder useSSE(boolean useSSE) {
			this.useSSE = useSSE;
			return this;
		}

		public Builder storageAccountName(CredentialValue storageAccountName) {
			this.storageAccountName = storageAccountName;
			return this;
		}

		public Builder storageAccountAccessKey(CredentialValue storageAccountAccessKey) {
			this.storageAccountAccessKey = storageAccountAccessKey;
			return this;
		}

		public Builder containerName(String containerName) {
			this.containerName = containerName;
			return this;
		}

		public Builder objectPath(String objectPath) {
			this.objectPath = objectPath;
			return this;
		}

		public WASBFileRef build() throws StageException {
			return new WASBFileRef(storageAccountName, storageAccountAccessKey, containerName, objectPath, useSSE);
		}
	}
}
