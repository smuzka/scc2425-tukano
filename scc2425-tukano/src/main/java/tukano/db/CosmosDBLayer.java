package tukano.db;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import tukano.api.ErrorResult;
import tukano.api.OkResult;
import tukano.api.Result;
import tukano.api.Result.ErrorCode;
import utils.PropsEnv;
import tukano.api.User;


import java.util.List;
import java.util.function.Supplier;


public class CosmosDBLayer {
	private static String DB_KEY;
	private static String CONNECTION_URL;
	private static String DB_NAME;

	private String containerName;
	
	private CosmosClient client;
	private CosmosDatabase db;
	private CosmosContainer container;
	
	public CosmosDBLayer(String containerName) {
		DB_KEY = PropsEnv.get("COSMOSDB_KEY", "");
		CONNECTION_URL = PropsEnv.get("COSMOSDB_URL", "");
		DB_NAME = PropsEnv.get("COSMOSDB_DATABASE", "");

		CosmosClient client = new CosmosClientBuilder()
				.endpoint(CONNECTION_URL)
				.key(DB_KEY)
				//.directMode()
				.gatewayMode()
				// replace by .directMode() for better performance
				.consistencyLevel(ConsistencyLevel.SESSION)
				.connectionSharingAcrossClientsEnabled(true)
				.contentResponseOnWriteEnabled(true)
				.buildClient();

		this.containerName = containerName;
		this.client = client;
	}
	
	private synchronized void init() {
		if( db != null)
			return;
		db = client.getDatabase(DB_NAME);
		container = db.getContainer(containerName);
	}

	public void close() {
		client.close();
	}
	
	public <T> Result<T> getOne(String id, Class<T> clazz) {
		return tryCatch( () -> container.readItem(id, new PartitionKey(id), clazz).getItem());
	}

	public Result<User> deleteUser(User obj) {
		return tryCatch( () -> (User)container.deleteItem(obj, new CosmosItemRequestOptions()).getItem());
	}

	public <T> Result<Void> deleteOneWithVoid(T obj) {
		try {
			CosmosItemResponse<Object> response = container.deleteItem(obj, new CosmosItemRequestOptions());
			return new OkResult<>(null);
		} catch (Exception e) {
			return new ErrorResult<>(ErrorCode.INTERNAL_ERROR);
		}
	}
	
	public <T> Result<T> updateOne(T obj) {
		return tryCatch( () -> container.upsertItem(obj).getItem());
	}
	
	public <T> Result<T> insertOne( T obj) {
		return tryCatch( () -> container.createItem(obj).getItem());
	}
	
	public <T> Result<List<T>> query(Class<T> clazz, String queryStr) {
		return tryCatch(() -> {
			var res = container.queryItems(queryStr, new CosmosQueryRequestOptions(), clazz);
			return res.stream().toList();
		});
	}
	
	<T> Result<T> tryCatch( Supplier<T> supplierFunc) {
		try {

			init();
			return Result.ok(supplierFunc.get());			
		} catch( CosmosException ce ) {
			//ce.printStackTrace();
			return Result.error ( errorCodeFromStatus(ce.getStatusCode() ));		
		} catch( Exception x ) {
			x.printStackTrace();
			return Result.error( ErrorCode.INTERNAL_ERROR);
		}
	}

	static Result.ErrorCode errorCodeFromStatus( int status ) {
		return switch( status ) {
		case 200 -> ErrorCode.OK;
		case 404 -> ErrorCode.NOT_FOUND;
		case 409 -> ErrorCode.CONFLICT;
		default -> ErrorCode.INTERNAL_ERROR;
		};
	}
}
