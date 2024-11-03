package tukano.impl;

import java.sql.Connection;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import tukano.api.ErrorResult;
import tukano.api.OkResult;
import tukano.api.Result;
import tukano.api.Short;
import tukano.api.Shorts;
import tukano.api.User;
import tukano.db.CosmosDBLayer;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import tukano.pojoModels.CountResult;
import tukano.pojoModels.Id;
import utils.CSVLogger;
import utils.SqlDB;

import static java.lang.String.format;
import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrResult;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.errorOrVoid;
import static tukano.api.Result.ok;

public class JavaShorts implements Shorts {

	private final static String REDIS_SHORTS = "shorts:";

	CSVLogger csvLogger = new CSVLogger();
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
			var blobUrl = shortId;
			var shrt = new Short(shortId, userId, blobUrl);
			var timestamp = System.currentTimeMillis();


			Result<Short> sqlResult = errorOrValue( SqlDB.insertOne(shrt), s -> s.copyWithLikes_And_Token(0, timestamp) );
			if (!sqlResult.isOK()) {
				Log.warning("Couldn't write to SQL DB");
				return errorOrValue(sqlResult, sqlResult.value());
			}

			Result<Short> cosmosResult = errorOrValue(cosmosDBLayerForShorts.insertOne(shrt), s -> s.copyWithLikes_And_Token(0, timestamp));
			if (cosmosResult.isOK()) {
				RedisJedisPool.addToCache(REDIS_SHORTS + shortId, shrt);
			}

			if(!sqlResult.value().equals(cosmosResult.value())){
				Log.warning("Results from cosmos and sql database doesn't match");
				return error(Result.ErrorCode.INTERNAL_ERROR);
			}

			return cosmosResult;
		});
	}

	@Override
	public Result<Short> getShort(String shortId) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("getShort : shortId = %s\n", shortId));
		var timestamp = System.currentTimeMillis();
		if( shortId == null )
			return error(BAD_REQUEST);

		var cosmosQuery = format("SELECT count(1) AS count FROM likes l WHERE l.shortId = '%s'", shortId);


		Short CacheShort = RedisJedisPool.getFromCache(REDIS_SHORTS + shortId, Short.class);
		if (CacheShort != null) {
			var likes = cosmosDBLayerForLikes.query(CountResult.class, cosmosQuery);
			var results = transformSingleResult(likes, CountResult::getCount);
			csvLogger.logToCSV("Get short with redis", System.currentTimeMillis() - startTime);
			return ok(CacheShort.copyWithLikes_And_Token( results.value()));
		}

		var sqlQuery = format("SELECT count(*) FROM Likes l WHERE l.shortId = '%s'", shortId);
		var sqlLikes = SqlDB.sql(sqlQuery, Long.class);
		Result<Short> sqlResult = errorOrValue( SqlDB.getOne(shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( sqlLikes.get(0), timestamp));
		if (!sqlResult.isOK()) {
			Log.warning("Couldn't get from SQL DB");
			return errorOrValue(sqlResult, sqlResult.value());
		}

		var likes = cosmosDBLayerForLikes.query(CountResult.class, cosmosQuery);
		var likesResultCosmo = transformSingleResult(likes, CountResult::getCount);
		Result<Short> cosmosResult = errorOrValue( cosmosDBLayerForShorts.getOne(shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( likesResultCosmo.value(), timestamp));

		if(!sqlResult.value().equals(cosmosResult.value())){
			Log.warning("Results from cosmos and sql database doesn't match");
			return error(Result.ErrorCode.INTERNAL_ERROR);
		}

		csvLogger.logToCSV("Get short without redis", System.currentTimeMillis() - startTime);
		return cosmosResult;
	}

	public Result<Short> getShortWithoutToken(String shortId) {
		Log.info(() -> format("getShort : shortId = %s\n", shortId));

		if( shortId == null )
			return error(BAD_REQUEST);

		return errorOrValue( cosmosDBLayerForShorts.getOne(shortId, Short.class), shrt -> shrt);
	}

	//todo: add sql
	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));


		//return  for sql
				errorOrResult( getShort(shortId), shrt -> {

			return errorOrResult( okUser( shrt.getOwnerId(), password), user -> {
				return SqlDB.transaction( hibernate -> {

					hibernate.remove( shrt);

					var query = format("DELETE FROM Likes WHERE shortId = '%s'", shortId);
					hibernate.createNativeQuery( query, Likes.class).executeUpdate();

					JavaBlobs.getInstance().delete(shrt.getBlobUrl(), Token.get(shrt.getBlobUrl()) );
				});
			});
		});

		return errorOrResult( getShortWithoutToken(shortId), shrt -> {

			return errorOrResult( okUser( shrt.getOwnerId(), password), user -> {

				String query = format("SELECT * FROM likes l WHERE l.shortId = '%s'", shortId);
				Result<List<Likes>> likesResult = cosmosDBLayerForLikes.query(Likes.class, query);

				if (likesResult.isOK()) {
					for (Likes like : likesResult.value()) {
						cosmosDBLayerForLikes.deleteOneWithVoid(like);
					}
				}
				else {
					return  new ErrorResult<>(Result.ErrorCode.INTERNAL_ERROR);
				}

				JavaBlobs.getInstance().delete(shrt.getBlobUrl(), Token.get(shrt.getBlobUrl()));

				Result<Void> removingShortResult = cosmosDBLayerForShorts.deleteOneWithVoid(shrt);
				if (removingShortResult.isOK()) {
					RedisJedisPool.removeFromCache(REDIS_SHORTS + shortId);
				}

				return removingShortResult;
			});
		});
	}

	@Override
	public Result<List<String>> getShorts(String userId, String pwd) {
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		var sqlQuery = format("SELECT s.id FROM Short s WHERE s.ownerId = '%s'", userId);
		var resultSql = errorOrValue( okUser(userId), SqlDB.sql( sqlQuery, String.class));

		var cosmosQuery = format("SELECT s.id FROM shorts s WHERE s.ownerId = '%s'", userId);
		var shortIdsCosmos = errorOrValue( okUser(userId), cosmosDBLayerForShorts.query( Id.class, cosmosQuery));
		Result <List<String>> result = transformResult(shortIdsCosmos, Id::getId);

		if(!new HashSet<>(resultSql.value()).equals(new HashSet<>(result.value()))){
			Log.warning("Results from cosmos and sql database doesn't match");
			return error(Result.ErrorCode.INTERNAL_ERROR);
		}

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
			var sqlResult = errorOrVoid( okUser( userId2), isFollowing ? SqlDB.insertOne( f ) : SqlDB.deleteOne( f ));
			if (!sqlResult.isOK()) {
				Log.warning("Couldn't write to SQL DB");
				return errorOrValue(sqlResult, sqlResult.value());
			}
			return errorOrVoid(  okUser(userId2), isFollowing ? cosmosDBLayerForFollowing.insertOne( f ) : cosmosDBLayerForFollowing.deleteOneWithVoid( f ));
		});			
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

		var sqlQuery = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);
		var resultSql = errorOrValue( okUser(userId, password), SqlDB.sql( sqlQuery, String.class));

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
			var sqlResult = errorOrVoid( okUser( userId, password), isLiked ? SqlDB.insertOne( l ) : SqlDB.deleteOne( l ));
			if(!sqlResult.isOK()){
				Log.warning("Couldn't write to SQL DB");
				return errorOrValue(sqlResult, sqlResult.value());
			}
			return errorOrVoid( okUser( userId, password), isLiked ? cosmosDBLayerForLikes.insertOne( l ) : cosmosDBLayerForLikes.deleteOneWithVoid( l ));
		});
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		Log.info(() -> format("likes : shortId = %s, pwd = %s\n", shortId, password));

		return errorOrResult( getShort(shortId), shrt -> {
			var sqlQuery = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);
			var resultSql = errorOrValue( okUser(shrt.getOwnerId(), password), SqlDB.sql( sqlQuery, String.class));
			if(!resultSql.isOK()){
				Log.warning("Couldn't get from SQL DB");
				return errorOrValue(resultSql, resultSql.value());
			}

			var query = format("SELECT l.userId as id FROM likes l WHERE l.shortId = '%s'", shortId);

			var usersIds = cosmosDBLayerForLikes.query(Id.class, query);
			Result <List<String>> result = transformResult(usersIds,Id::getId);
			
			return errorOrValue( okUser( shrt.getOwnerId(), password ), result);
		});
	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));


		final var QUERY_FMT = """
				SELECT s.id, s.timestamp FROM Short s WHERE	s.ownerId = '%s'				
				UNION			
				SELECT s.id, s.timestamp FROM Short s, Following f 
					WHERE 
						f.followee = s.ownerId AND f.follower = '%s' 
				ORDER BY s.timestamp DESC""";
		//return for sql
		errorOrValue( okUser( userId, password), SqlDB.sql( format(QUERY_FMT, userId, userId), String.class));

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

	@Override
	public Result<Void> deleteAllShorts(String userId, String password, String token) {


		SqlDB.transaction( (hibernate) -> {

			var queryShorts = format("SELECT s FROM Short s WHERE s.ownerId = '%s'", userId);
			List<Short> shortsToDelete = hibernate.createQuery(queryShorts, Short.class).getResultList();

			for (Short shortItem : shortsToDelete) {
				Result<Void> deleteBlobResult = JavaBlobs.getInstance().delete(shortItem.getBlobUrl(), Token.get(shortItem.getBlobUrl()));
				if (deleteBlobResult.isOK()) {
					RedisJedisPool.removeFromCache(REDIS_SHORTS + shortItem.getId());
				}
			}

			//delete shorts
			var query1 = format("DELETE FROM Short WHERE ownerId = '%s'", userId);
			hibernate.createNativeQuery(query1, Short.class).executeUpdate();

			//delete likes
			var query3 = format("DELETE FROM Likes WHERE ownerId = '%s' OR userId = '%s'", userId, userId);
			hibernate.createNativeQuery(query3, Likes.class).executeUpdate();

		});


		Log.info(() -> format("deleteAllShorts : userId = %s, password = %s, token = %s\n", userId, password, token));

		var shortQuery = String.format("SELECT * FROM shorts s WHERE s.ownerId = '%s'", userId);
		var shortsQueryResult = cosmosDBLayerForShorts.query(Short.class, shortQuery);
		if (!shortsQueryResult.isOK()) {
			return new ErrorResult<>(shortsQueryResult.error());
		}
		List<String> shortsList = transformResult(shortsQueryResult, Short::getId).value();

		String shortIds = "";
		if (!shortsList.isEmpty()) {
			shortIds = shortsList.stream()
					.map(id -> "'" + id + "'")
					.collect(Collectors.joining(","));
		}

		String query = format("SELECT * FROM likes l WHERE l.shortId IN (%s)", shortIds);
		Result<List<Likes>> likesResult = cosmosDBLayerForLikes.query(Likes.class, query);
		if (!likesResult.isOK()) {
			return new ErrorResult<>(likesResult.error());
		}

		for (Likes likeItem : likesResult.value()) {
			Result<Void> deleteResult = cosmosDBLayerForLikes.deleteOneWithVoid(likeItem);
			if (!deleteResult.isOK()) {
				return new ErrorResult<>(deleteResult.error());
			}
		}
		for (Short shortItem : shortsQueryResult.value()) {
			Result<Void> deleteBlobResult = JavaBlobs.getInstance().delete(shortItem.getBlobUrl(), Token.get(shortItem.getBlobUrl()));
			Result<Void> deleteShortResult = cosmosDBLayerForShorts.deleteOneWithVoid(shortItem);

			if(deleteBlobResult.isOK()) {
				RedisJedisPool.removeFromCache(REDIS_SHORTS + shortItem.getId());
			}

			if (!deleteShortResult.isOK() || !deleteBlobResult.isOK()) {
				return new ErrorResult<>(deleteShortResult.error());
			}
		}

		return ok();
	}
	
}