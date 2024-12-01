package tukano.impl.storage;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.function.Consumer;

import tukano.api.Result;
import utils.CSVLogger;
import utils.PropsEnv;

import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.ErrorCode.INTERNAL_ERROR;
import static tukano.api.Result.error;
import static tukano.api.Result.ok;

public class AzureClientStorage implements BlobStorage {

    private static AzureClientStorage instance;
    private BlobContainerClient containerClient;
    private String storageConnectionString;
    CSVLogger csvLogger = new CSVLogger();

    public AzureClientStorage() {
//        storageConnectionString = PropsEnv.get("BLOB_STORAGE_CONNECTION", "");
        storageConnectionString = System.getenv("BLOB_STORAGE_CONNECTION");

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

            byte[] result = azureClientContainer.getContainerClient(path).downloadContent().toBytes();
            triggerReadingBlobFunction(path);

            return ok(result);
        } catch (Exception e) {
            e.printStackTrace();
            return error(INTERNAL_ERROR);
        }
    }

    private void triggerReadingBlobFunction (String blobName) {

//        String FUNCTION_URL = PropsEnv.get("FUNCTION_URL", "");
//        String FUNCTION_KEY = PropsEnv.get("FUNCTION_KEY", "");
        String FUNCTION_URL = System.getenv("FUNCTION_URL");
        String FUNCTION_KEY = System.getenv("FUNCTION_KEY");

        String urlString = FUNCTION_URL + "&blobname=" + blobName;

        try {
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("x-functions-key", FUNCTION_KEY);

            int responseCode = conn.getResponseCode();
            String responseMessage = conn.getResponseMessage();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                System.out.println("Function triggered successfully. Response code: " + responseCode);
            } else {
                System.out.println("Failed to trigger a function. Response code: " + responseCode);
            }

        } catch (Exception e) {
            e.printStackTrace();
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