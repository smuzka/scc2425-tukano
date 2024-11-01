package tukano.db;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import tukano.api.Result;
import tukano.api.Result.ErrorCode;


import java.util.List;
import java.util.function.Supplier;


public class CosmosDBLayer {
	private static final String DB_KEY = "...";
	private static final String CONNECTION_URL = "...";
	private static final String DB_NAME = "...";
	private String containerName;

//	private static final String DB_KEY = System.getProperty("COSMOSDB_KEY");
//	private static final String CONNECTION_URL = 	System.getProperty("COSMOSDB_URL");
//	private static final String DB_NAME = 	System.getProperty("COSMOSDB_DATABASE");
//	static String CONTAINER_NAME = "users";



//	private static CosmosDBLayer instance;

//	public static synchronized CosmosDBLayer getInstance() {
//		System.out.println(DB_KEY);
//
//		instance = new CosmosDBLayer( client);
//		return instance;
//
//	}
	
	private CosmosClient client;
	private CosmosDatabase db;
	private CosmosContainer container;
	
	public CosmosDBLayer(String containerName) {
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
	
	public <T> Result<?> deleteOne(T obj) {
		return tryCatch( () -> container.deleteItem(obj, new CosmosItemRequestOptions()).getItem());
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