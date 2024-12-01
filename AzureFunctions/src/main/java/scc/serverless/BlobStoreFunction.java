package scc.serverless;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.util.Map;
import java.util.Optional;


public class BlobStoreFunction {
	private static final String BLOBS_FUNCTION_NAME = "blobFunctionTukano";

	private static final String COSMOS_DB_URI = "CosmosDBUri";
	private static final String COSMOS_DB_KEY = "CosmosDbKey";
	private static final String DATABASE_NAME = "DatabaseName";
	private static final String CONTAINER_NAME = "ContainerName";

	private static final CosmosAsyncClient cosmosClient = new CosmosClientBuilder()
			.endpoint(COSMOS_DB_URI)
			.key(COSMOS_DB_KEY)
			.gatewayMode()
			.consistencyLevel(ConsistencyLevel.SESSION)
			.connectionSharingAcrossClientsEnabled(true)
			.contentResponseOnWriteEnabled(true)
			.buildAsyncClient();

	private static final CosmosAsyncContainer container = cosmosClient
			.getDatabase(DATABASE_NAME)
			.getContainer(CONTAINER_NAME);

	@FunctionName(BLOBS_FUNCTION_NAME)
	public void blobFunctionTukano(
			@HttpTrigger(name = "req", methods = {HttpMethod.GET}, authLevel = AuthorizationLevel.FUNCTION) HttpRequestMessage<Optional<String>> request,
			final ExecutionContext context) {

		Map<String, String> queryParameters = request.getQueryParameters();
		context.getLogger().info("GET parameters are: " + queryParameters);
		String blobname = queryParameters.getOrDefault("blobname", "");
		context.getLogger().info("Blobname: " + blobname);
		String ItemId = blobname.replace('/', '+');
		context.getLogger().info("ItemId: " + ItemId);

		try {
			CosmosItemResponse<Short> response = container.readItem(ItemId, new PartitionKey(ItemId), Short.class).block();
			if (response != null) {
				context.getLogger().severe("Read item response: " + response.getItem());
				Short existingItem = (Short) response.getItem();
				existingItem.incrementTotalViews();

				CosmosItemResponse<Short> updateResponse = container.replaceItem(
						existingItem,
						ItemId,
						new PartitionKey(ItemId),
						new CosmosItemRequestOptions()
				).block();

				context.getLogger().severe("Update response: " + updateResponse);
				context.getLogger().severe("Incremented short views with blobId: " + ItemId);
			} else {
				context.getLogger().severe("Failed to find short with blobId: " + ItemId);
			}

		} catch (CosmosException e) {
			context.getLogger().severe("Failed to update item with id: " + ItemId + "\nError message: " + e.getMessage());
		}
	}
}
