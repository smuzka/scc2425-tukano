package tukano.impl.storage;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import java.util.function.Consumer;

import tukano.api.Result;

import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.Result.error;
import static tukano.api.Result.ok;

public class AzureClientStorage implements BlobStorage {

    private static AzureClientStorage instance;
    private BlobContainerClient containerClient;
    private final String storageConnectionString = "...";

    public AzureClientStorage() {
        containerClient = new BlobContainerClientBuilder()
                .connectionString(storageConnectionString)
                .containerName("shorts")
                .buildClient();
    }

    public static synchronized AzureClientStorage getInstance() {
        if (instance == null) {
            instance = new AzureClientStorage();
        }
        return instance;
    }

    public BlobClient getContainerClient(String blobName) {
        return containerClient.getBlobClient(blobName);
    }

    @Override
    public Result<Void> write(String path, byte[] bytes) {
        if (path == null)
            return error(BAD_REQUEST);

        try {
            BinaryData data = BinaryData.fromBytes(bytes);
            AzureClientStorage azureClientContainer = AzureClientStorage.getInstance();

            azureClientContainer.getContainerClient(path).upload(data);

            return ok();

        } catch (Exception e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> delete(String path) {
        if (path == null)
            return error(BAD_REQUEST);

        try {
            AzureClientStorage azureClientContainer = AzureClientStorage.getInstance();
            azureClientContainer.getContainerClient(path).delete();

            return ok();
        } catch (Exception e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<byte[]> read(String path) {
        if (path == null)
            return error(BAD_REQUEST);

        try {
            AzureClientStorage azureClientContainer = AzureClientStorage.getInstance();

            return ok(azureClientContainer.getContainerClient(path).downloadContent().toBytes());

        } catch (Exception e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    @Override
    public Result<Void> read(String path, Consumer<byte[]> sink) {
        if (path == null)
            return error(BAD_REQUEST);

        try {
            AzureClientStorage azureClientContainer = AzureClientStorage.getInstance();
            sink.accept(azureClientContainer.getContainerClient(path).downloadContent().toBytes());

            return ok();
        } catch (Exception e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }
}