package tukano.impl;

import java.util.*;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

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
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

        return errorOrResult( okUser(userId, password), user -> {

			var shortId = format("%s+%s", userId, UUID.randomUUID());
			var blobUrl = shortId;
			var shrt = new Short(shortId, userId, blobUrl);

			Result<Short> cosmosResult = errorOrValue(cosmosDBLayerForShorts.insertOne(shrt), s -> s.copyWithLikes_And_Token(0));
			csvLogger.logToCSV("create short", System.currentTimeMillis() - startTime);
			if (cosmosResult.isOK()) {
				RedisJedisPool.addToCache(REDIS_SHORTS + shortId, shrt);
			}

			return cosmosResult;
		});
	}

	@Override
	public Result<Short> getShort(String shortId) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("getShort : shortId = %s\n", shortId));

		if( shortId == null )
			return error(BAD_REQUEST);

		var cosmosQuery = format("SELECT count(1) AS count FROM likes l WHERE l.shortId = '%s'", shortId);


		Short cacheShort = RedisJedisPool.getFromCache(REDIS_SHORTS + shortId, Short.class);
		var likes = cosmosDBLayerForLikes.query(CountResult.class, cosmosQuery);
		var result= transformSingleResult(likes, CountResult::getCount);
		if (cacheShort != null) {

			csvLogger.logToCSV("Get short with redis", System.currentTimeMillis() - startTime);
			return ok(cacheShort.copyWithLikes_And_Token( result.value()));
		}

		Result<Short> cosmosResult = errorOrValue( cosmosDBLayerForShorts.getOne(shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( result.value()));

		csvLogger.logToCSV("Get short without redis", System.currentTimeMillis() - startTime);
		return cosmosResult;
	}

	public Result<Short> getShortWithoutToken(String shortId) {
		Log.info(() -> format("getShort : shortId = %s\n", shortId));

		if( shortId == null )
			return error(BAD_REQUEST);

		return errorOrValue( cosmosDBLayerForShorts.getOne(shortId, Short.class), shrt -> shrt);
	}

	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));

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
				csvLogger.logToCSV("delete short", System.currentTimeMillis() - startTime);

				if (removingShortResult.isOK()) {
					RedisJedisPool.removeFromCache(REDIS_SHORTS + shortId);
				}

				return removingShortResult;
			});
		});
	}

	@Override
	public Result<List<String>> getShorts(String userId, String pwd) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("getShorts : userId = %s\n", userId));

		var cosmosQuery = format("SELECT s.id FROM shorts s WHERE s.ownerId = '%s'", userId);
		var shortIdsCosmos = errorOrValue( okUser(userId), cosmosDBLayerForShorts.query( Id.class, cosmosQuery));
		Result <List<String>> result = transformResult(shortIdsCosmos, Id::getId);
		csvLogger.logToCSV("get all shorts", System.currentTimeMillis() - startTime);

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
			return errorOrVoid(  okUser(userId2), isFollowing ? cosmosDBLayerForFollowing.insertOne( f ) : cosmosDBLayerForFollowing.deleteOneWithVoid( f ));
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
			return errorOrVoid( okUser( userId, password), isLiked ? cosmosDBLayerForLikes.insertOne( l ) : cosmosDBLayerForLikes.deleteOneWithVoid( l ));
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
		long startTime = System.currentTimeMillis();
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
		csvLogger.logToCSV("Get feed", System.currentTimeMillis() - startTime);

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
		long startTime = System.currentTimeMillis();
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
		csvLogger.logToCSV("delete all shorts", System.currentTimeMillis() - startTime);
		return ok();
	}
	
}