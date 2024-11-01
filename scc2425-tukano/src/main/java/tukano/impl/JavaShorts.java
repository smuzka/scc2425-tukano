package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrResult;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.errorOrVoid;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.ErrorCode.FORBIDDEN;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import tukano.api.*;
import tukano.api.Short;
import tukano.db.CosmosDBLayer;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import tukano.impl.rest.TukanoRestServer;
import tukano.pojoModels.CountResult;
import tukano.pojoModels.Id;
import utils.DB;

public class JavaShorts implements Shorts {

	private static Logger Log = Logger.getLogger(JavaShorts.class.getName());
	CosmosDBLayer cosmosDBLayerForShorts = new CosmosDBLayer("shorts");
	CosmosDBLayer cosmosDBLayerForLikes = new CosmosDBLayer("likes");
	CosmosDBLayer cosmosDBLayerForFollowing = new CosmosDBLayer("following");

	private static Shorts instance;
	
	synchronized public static Shorts getInstance() {
		if( instance == null )
			instance = new JavaShorts();
		return instance;
	}
	
	private JavaShorts() {}
	
	
	@Override
	public Result<Short> createShort(String userId, String password) {
		Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

		return errorOrResult( okUser(userId, password), user -> {
			
			var shortId = format("%s+%s", userId, UUID.randomUUID());
			var blobUrl = format("%s/%s/%s", TukanoRestServer.serverURI, Blobs.NAME, shortId); 
			var shrt = new Short(shortId, userId, blobUrl);

			return errorOrValue(cosmosDBLayerForShorts.insertOne(shrt), s -> s.copyWithLikes_And_Token(0));
		});
	}

	@Override
	public Result<Short> getShort(String shortId) {
		Log.info(() -> format("getShort : shortId = %s\n", shortId));

		if( shortId == null )
			return error(BAD_REQUEST);


		var query = format("SELECT count(1) AS count FROM likes l WHERE l.shortId = '%s'", shortId);
		var likes = cosmosDBLayerForLikes.query(CountResult.class, query);
		var results = transformSingleResult(likes, CountResult::getCount);

		return errorOrValue( cosmosDBLayerForShorts.getOne(shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( results.value()));
	}

	//todo change to cosmo delete
	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));
		
		return errorOrResult( getShort(shortId), shrt -> {
			
			return errorOrResult( okUser( shrt.getOwnerId(), password), user -> {
				return DB.transaction( hibernate -> {

					hibernate.remove( shrt);
					
					var query = format("DELETE Likes l WHERE l.shortId = '%s'", shortId);
					hibernate.createNativeQuery( query, Likes.class).executeUpdate();
					
					JavaBlobs.getInstance().delete(shrt.getBlobUrl(), Token.get() );
				});
			});	
		});
	}

	@Override
	public Result<List<String>> getShorts(String userId, String pwd) {
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		var query = format("SELECT s.id FROM shorts s WHERE s.ownerId = '%s'", userId);

		var shortIds = cosmosDBLayerForShorts.query( Id.class, query);
		Result <List<String>> result = transformResult(shortIds, Id::getId);

		return errorOrValue( okUser(userId, pwd), result);
	}

	private <T> Result<List<String>> transformResult(Result<List<T>> inputResult, Function<T, String> mapper) {
		if (inputResult.isOK()) {
			List<String> transformedList = inputResult.value().stream()
					.map(mapper)
					.collect(Collectors.toList());
			return new OkResult<>(transformedList);
		} else {
			return new ErrorResult<>(inputResult.error());
		}
	}

	private <T> Result<Long> transformSingleResult(Result<List<T>> inputResult, Function<T, Long> mapper) {
		if (inputResult.isOK()) {
			Long transformedValue = mapper.apply(inputResult.value().get(0));
			return new OkResult<>(transformedValue);
		} else {
			return new ErrorResult<>(inputResult.error());
		}
	}

	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		Log.info(() -> format("follow : userId1 = %s, userId2 = %s, isFollowing = %s, pwd = %s\n", userId1, userId2, isFollowing, password));

		return errorOrResult( okUser(userId1, password), user -> {
			var f = new Following( userId1 +"+" +userId2,userId1, userId2);
			return errorOrVoid(  okUser(userId2), isFollowing ? cosmosDBLayerForFollowing.insertOne( f ) : cosmosDBLayerForFollowing.deleteOne( f ));
		});			
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

		var query = format("SELECT f.follower AS id FROM following f WHERE f.followee = '%s'", userId);
		var shortIds = cosmosDBLayerForFollowing.query( Id.class, query);
		Result <List<String>> result = transformResult(shortIds, Id::getId);

		return errorOrValue( okUser(userId, password),result);
	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s, pwd = %s\n", shortId, userId, isLiked, password));

		
		return errorOrResult( getShort(shortId), shrt -> {
			var l = new Likes(userId, shortId, shrt.getOwnerId());
			return errorOrVoid( okUser( userId, password), isLiked ? cosmosDBLayerForLikes.insertOne( l ) : cosmosDBLayerForLikes.deleteOne( l ));
		});
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		Log.info(() -> format("likes : shortId = %s, pwd = %s\n", shortId, password));

		return errorOrResult( getShort(shortId), shrt -> {
			
			var query = format("SELECT l.userId as id FROM likes l WHERE l.shortId = '%s'", shortId);

			var usersIds = cosmosDBLayerForLikes.query(Id.class, query);
			Result <List<String>> result = transformResult(usersIds,Id::getId);
			
			return errorOrValue( okUser( shrt.getOwnerId(), password ), result);
		});
	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));

		var ownerShortsResult = cosmosDBLayerForShorts.query(Short.class, String.format("SELECT s.id, s.timestamp FROM shorts s WHERE s.ownerId = '%s'", userId));
		var followeListResult = cosmosDBLayerForFollowing.query(Following.class, String.format("SELECT f.followee FROM following f WHERE f.follower = '%s'", userId));
		List<String> followeeIds = followeListResult.value().stream().map(Following::getFollowee).collect(Collectors.toList());
		List<Short> combinedResults = new ArrayList<>(ownerShortsResult.value());

		if (!followeeIds.isEmpty()) {
			String followeeIdsString = followeeIds.stream()
					.map(id -> "'" + id + "'")
					.collect(Collectors.joining(","));
			var followedShortsResult = cosmosDBLayerForShorts.query(Short.class, String.format("SELECT s.id, s.timestamp FROM shorts s WHERE s.ownerId IN (%s)", followeeIdsString));
			if (followedShortsResult.isOK()) {
				combinedResults.addAll(followedShortsResult.value());
			}
		}

		combinedResults.sort(Comparator.comparing(Short::getTimestamp).reversed());
		List<String> shortIds = combinedResults.stream()
				.map(Short::getId)
				.collect(Collectors.toList());

		return errorOrValue( okUser( userId, password), shortIds);
	}
		
	protected Result<User> okUser( String userId, String pwd) {
		return JavaUsers.getInstance().getUser(userId, pwd);
	}

	protected Result<User> okUserWithoutPwd( String userId) {
		return JavaUsers.getInstance().getUserWithoutPwd(userId);
	}


	private Result<Void> okUser( String userId ) {
		var res = okUserWithoutPwd(userId);
		if( res.isOK())
			return ok();
		else
			return error( res.error() );
	}


	//todo: change to cosmoDB
	@Override
	public Result<Void> deleteAllShorts(String userId, String password, String token) {
		Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));

		if( ! Token.isValid( token, userId ) )
			return error(FORBIDDEN);
		
		return DB.transaction( (hibernate) -> {
						
			//delete shorts
			var query1 = format("DELETE Short s WHERE s.ownerId = '%s'", userId);		
			hibernate.createQuery(query1, Short.class).executeUpdate();
			
			//delete follows
			var query2 = format("DELETE Following f WHERE f.follower = '%s' OR f.followee = '%s'", userId, userId);		
			hibernate.createQuery(query2, Following.class).executeUpdate();
			
			//delete likes
			var query3 = format("DELETE Likes l WHERE l.ownerId = '%s' OR l.userId = '%s'", userId, userId);		
			hibernate.createQuery(query3, Likes.class).executeUpdate();
			
		});
	}
	
}