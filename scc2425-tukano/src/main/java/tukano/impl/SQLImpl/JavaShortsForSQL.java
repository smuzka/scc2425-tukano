package tukano.impl.SQLImpl;

import tukano.api.Short;
import tukano.api.*;
import tukano.impl.JavaBlobs;
//import tukano.impl.RedisJedisPool;
import tukano.impl.Token;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import utils.CSVLogger;
import utils.SqlDB;

import java.util.*;
import java.util.logging.Logger;

import static java.lang.String.format;
import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.*;

public class JavaShortsForSQL implements Shorts {

//	private final static String REDIS_SHORTS = "shorts:";

	CSVLogger csvLogger = new CSVLogger();
	private static Logger Log = Logger.getLogger(JavaShortsForSQL.class.getName());
	private static Shorts instance;

	synchronized public static Shorts getInstance() {
		if( instance == null )
			instance = new JavaShortsForSQL();
		return instance;
	}

	private JavaShortsForSQL() {
		System.out.println("==========SQLDB SOLUTION FOR SHORTS============");
	}

	@Override
	public Result<Short> createShort(String userId, String password) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("createShort : userId = %s, pwd = %s\n", userId, password));

        return errorOrResult( okUser(userId, password), user -> {

			var shortId = format("%s+%s", userId, UUID.randomUUID());
			var blobUrl = shortId;
			var shrt = new Short(shortId, userId, blobUrl);

			Result<Short> result = errorOrValue(SqlDB.insertOne(shrt), s -> s.copyWithLikes_And_Token(0));
			csvLogger.logToCSV("create short", System.currentTimeMillis() - startTime);
//			if (result.isOK()) {
//				RedisJedisPool.addToCache(REDIS_SHORTS + shortId, shrt);
//			}

			return result;
		});
	}

	@Override
	public Result<Short> getShort(String shortId) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("getShort : shortId = %s\n", shortId));
		if( shortId == null )
			return error(BAD_REQUEST);

		var sqlQuery = format("SELECT count(*) FROM Likes l WHERE l.shortId = '%s'", shortId);
		var sqlLikes = SqlDB.sql(sqlQuery, Long.class);

//		Short CacheShort = RedisJedisPool.getFromCache(REDIS_SHORTS + shortId, Short.class);
//		if (CacheShort != null) {
//			csvLogger.logToCSV("Get short with redis", System.currentTimeMillis() - startTime);
//			return ok(CacheShort.copyWithLikes_And_Token( sqlLikes.get(0)));
//		}

		Result<Short> result = errorOrValue( SqlDB.getOne(shortId, Short.class), shrt -> shrt.copyWithLikes_And_Token( sqlLikes.get(0)));
		csvLogger.logToCSV("Get short without redis", System.currentTimeMillis() - startTime);

		return result;
	}

	@Override
	public Result<Void> deleteShort(String shortId, String password) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("deleteShort : shortId = %s, pwd = %s\n", shortId, password));

		return errorOrResult( getShort(shortId), shrt -> {
			return errorOrResult( okUser( shrt.getOwnerId(), password), user -> {
				return SqlDB.transaction( hibernate -> {
					hibernate.remove( shrt);
					var query = format("DELETE FROM Likes WHERE shortId = '%s'", shortId);
					JavaBlobs.getInstance().delete(shrt.getBlobUrl(), Token.get(shrt.getBlobUrl()) );
					hibernate.createNativeQuery( query, Likes.class).executeUpdate();
					csvLogger.logToCSV("delete short", System.currentTimeMillis() - startTime);
//					RedisJedisPool.removeFromCache(REDIS_SHORTS + shortId);
				});
			});
		});
	}

	@Override
	public Result<List<String>> getShorts(String userId, String pwd) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("getShorts : userId = %s\n", userId));
		var sqlQuery = format("SELECT s.id FROM Short s WHERE s.ownerId = '%s'", userId);
		var result = errorOrValue( okUser(userId, pwd), SqlDB.sql( sqlQuery, String.class));
		csvLogger.logToCSV("get all shorts", System.currentTimeMillis() - startTime);
		return result;
	}

	@Override
	public Result<Void> follow(String userId1, String userId2, boolean isFollowing, String password) {
		Log.info(() -> format("follow : userId1 = %s, userId2 = %s, isFollowing = %s, pwd = %s\n", userId1, userId2, isFollowing, password));

		return errorOrResult( okUser(userId1, password), user -> {
			var f = new Following( userId1 +"+" +userId2,userId1, userId2);
			return errorOrVoid( okUser( userId2), isFollowing ? SqlDB.insertOne( f ) : SqlDB.deleteOne( f ));
		});			
	}

	@Override
	public Result<List<String>> followers(String userId, String password) {
		Log.info(() -> format("followers : userId = %s, pwd = %s\n", userId, password));

		var sqlQuery = format("SELECT f.follower FROM Following f WHERE f.followee = '%s'", userId);
		return errorOrValue( okUser(userId, password), SqlDB.sql( sqlQuery, String.class));
	}

	@Override
	public Result<Void> like(String shortId, String userId, boolean isLiked, String password) {
		Log.info(() -> format("like : shortId = %s, userId = %s, isLiked = %s, pwd = %s\n", shortId, userId, isLiked, password));

		return errorOrResult( getShort(shortId), shrt -> {
			var l = new Likes(userId, shortId, shrt.getOwnerId());
			return errorOrVoid( okUser( userId, password), isLiked ? SqlDB.insertOne( l ) : SqlDB.deleteOne( l ));
		});
	}

	@Override
	public Result<List<String>> likes(String shortId, String password) {
		Log.info(() -> format("likes : shortId = %s, pwd = %s\n", shortId, password));

		return errorOrResult( getShort(shortId), shrt -> {
			var sqlQuery = format("SELECT l.userId FROM Likes l WHERE l.shortId = '%s'", shortId);
			return errorOrValue( okUser(shrt.getOwnerId(), password), SqlDB.sql( sqlQuery, String.class));
		});
	}

	@Override
	public Result<List<String>> getFeed(String userId, String password) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("getFeed : userId = %s, pwd = %s\n", userId, password));

		final var QUERY_FMT = """
				SELECT s.id, s.timestamp FROM Short s WHERE	s.ownerId = '%s'				
				UNION			
				SELECT s.id, s.timestamp FROM Short s, Following f 
					WHERE 
						f.followee = s.ownerId AND f.follower = '%s' 
				ORDER BY s.timestamp DESC""";

		var result = SqlDB.sql( format(QUERY_FMT, userId, userId), String.class);
		csvLogger.logToCSV("Get feed", System.currentTimeMillis() - startTime);

		return errorOrValue( okUser( userId, password), result );
	}
		
	protected Result<User> okUser( String userId, String pwd) {
		return JavaUsersForSQL.getInstance().getUser(userId, pwd);
	}

	protected Result<User> okUserWithoutPwd( String userId) {
		return JavaUsersForSQL.getInstance().getUserWithoutPwd(userId);
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

		return SqlDB.transaction( (hibernate) -> {

			var queryShorts = format("SELECT s FROM Short s WHERE s.ownerId = '%s'", userId);
			List<Short> shortsToDelete = hibernate.createQuery(queryShorts, Short.class).getResultList();

			for (Short shortItem : shortsToDelete) {
				Result<Void> deleteBlobResult = JavaBlobs.getInstance().delete(shortItem.getBlobUrl(), Token.get(shortItem.getBlobUrl()));
//				if (deleteBlobResult.isOK()) {
//					RedisJedisPool.removeFromCache(REDIS_SHORTS + shortItem.getId());
//				}
			}

			//delete shorts
			var query1 = format("DELETE FROM Short WHERE ownerId = '%s'", userId);
			hibernate.createNativeQuery(query1, Short.class).executeUpdate();

			//delete likes
			var query3 = format("DELETE FROM Likes WHERE ownerId = '%s' OR userId = '%s'", userId, userId);
			hibernate.createNativeQuery(query3, Likes.class).executeUpdate();
			csvLogger.logToCSV("delete all shorts", System.currentTimeMillis() - startTime);
		});
	}
	
}