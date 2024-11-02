package tukano.impl;

import static java.lang.String.format;
import static tukano.api.Result.error;
import static tukano.api.Result.errorOrResult;
import static tukano.api.Result.errorOrValue;
import static tukano.api.Result.ok;
import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.ErrorCode.FORBIDDEN;

import java.util.List;
import java.util.logging.Logger;

import tukano.api.Result;
import tukano.api.User;
import tukano.api.Users;
import tukano.db.CosmosDBLayer;

public class JavaUsers implements Users {

	private final static String REDIS_USERS = "users:";

	private static Logger Log = Logger.getLogger(JavaUsers.class.getName());
	private static Users instance;
	private final CosmosDBLayer cosmosDBLayer = new CosmosDBLayer("users");

	synchronized public static Users getInstance() {
		if( instance == null )
			instance = new JavaUsers();
		return instance;
	}
	
	private JavaUsers() {}
	
	@Override
	public Result<String> createUser(User user) {
		Log.info(() -> format("createUser : %s\n", user));

		if( badUserInfo( user ) )
			return error(BAD_REQUEST);

		Result<String> result = errorOrValue( cosmosDBLayer.insertOne(user), user.getId() );
		if (result.isOK()) {
			RedisJedisPool.addToCache(REDIS_USERS + user.getId(), user);
		}

		return result;
	}

	@Override
	public Result<User> getUser(String userId, String pwd) {
		Log.info( () -> format("getUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null)
			return error(BAD_REQUEST);

		User CacheUser = RedisJedisPool.getFromCache(userId, User.class);
		if (CacheUser != null) {
			return ok(CacheUser);
		}

		return validatedUserOrError( cosmosDBLayer.getOne( userId, User.class), pwd);
	}

	@Override
	public Result<User> getUserWithoutPwd(String userId) {
		Log.info( () -> format("getUser : userId = %s, pwd = *no pwd*\n", userId));

		if (userId == null)
			return error(BAD_REQUEST);

		return cosmosDBLayer.getOne( userId, User.class);
	}

	@Override
	public Result<User> updateUser(String userId, String pwd, User other) {
		Log.info(() -> format("updateUser : userId = %s, pwd = %s, user: %s\n", userId, pwd, other));

		if (badUpdateUserInfo(userId, pwd, other))
			return error(BAD_REQUEST);

		Result<User> result = errorOrResult( validatedUserOrError(cosmosDBLayer.getOne( userId, User.class), pwd), user -> cosmosDBLayer.updateOne( user.updateFrom(other)));

		if (result.isOK()) {
			RedisJedisPool.addToCache(REDIS_USERS + userId, result.value());
		}

		return result;
	}

	//todo: add deltion of shorts and blobs
	@Override
	public Result<User> deleteUser(String userId, String pwd) {
		Log.info(() -> format("deleteUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null || pwd == null )
			return error(BAD_REQUEST);
//
		return  errorOrResult( validatedUserOrError(cosmosDBLayer.getOne( userId, User.class), pwd), user -> {

			// Delete user shorts and related info asynchronously in a separate thread
//			Executors.defaultThreadFactory().newThread( () -> {
//				JavaShorts.getInstance().deleteAllShorts(userId, pwd, Token.get(userId));
//				JavaBlobs.getInstance().deleteAllBlobs(userId, Token.get(userId));
//			}).start();

			Result<User> result = errorOrValue( cosmosDBLayer.deleteOne( user), user);
			if (result.isOK()) {
				RedisJedisPool.removeFromCache(REDIS_USERS + userId);
			}

			return result;
		});
	}

	@Override
	public Result<List<User>> searchUsers(String pattern) {
		Log.info( () -> format("searchUsers : patterns = %s\n", pattern));

		var query = format("SELECT * FROM User u WHERE UPPER(u.id) LIKE '%%%s%%'", pattern.toUpperCase());
		var hits = cosmosDBLayer.query(User.class, query)
				.value()
				.stream()
				.map(User::copyWithoutPassword)
				.toList();

		return ok(hits);
	}

	
	private Result<User> validatedUserOrError( Result<User> res, String pwd ) {
		if( res.isOK())
			return res.value().getPwd().equals( pwd ) ? res : error(FORBIDDEN);
		else
			return res;
	}
	
	private boolean badUserInfo( User user) {
		return (user.userId() == null || user.pwd() == null || user.displayName() == null || user.email() == null);
	}
	
	private boolean badUpdateUserInfo( String userId, String pwd, User info) {
		return (userId == null || pwd == null || info.getId() != null && ! userId.equals( info.getId()));
	}
}
