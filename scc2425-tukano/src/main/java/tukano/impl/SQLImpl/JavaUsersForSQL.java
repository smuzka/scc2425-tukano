package tukano.impl.SQLImpl;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import tukano.api.Result;
import tukano.api.User;
import tukano.api.Users;
//import tukano.impl.RedisJedisPool;
import tukano.impl.Token;
import utils.CSVLogger;
import utils.SqlDB;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import static java.lang.String.format;
import static tukano.api.Result.ErrorCode.BAD_REQUEST;
import static tukano.api.Result.ErrorCode.FORBIDDEN;
import static tukano.api.Result.*;

public class JavaUsersForSQL implements Users {

//	private final static String REDIS_USERS = "users:";

	CSVLogger csvLogger = new CSVLogger();
	private static Logger Log = Logger.getLogger(JavaUsersForSQL.class.getName());
	private static Users instance;

	synchronized public static Users getInstance() {
		if( instance == null )
			instance = new JavaUsersForSQL();
		return instance;
	}
	
	private JavaUsersForSQL() {
		System.out.println("==========SQLDB SOLUTION FOR USERS============");
	}
	
	@Override
	public Result<String> createUser(User user) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("createUser : %s\n", user));

		if( badUserInfo( user ) )
			return error(BAD_REQUEST);

		Result<String> result = errorOrValue( SqlDB.insertOne(user), user.getId());
//		if (result.isOK()) {
//			RedisJedisPool.addToCache(REDIS_USERS + user.getId(), user);
//		}
		csvLogger.logToCSV("Create user ", System.currentTimeMillis() - startTime);
		return result;
	}

	@Override
	public Result<User> getUser(String userId, String pwd) {
		long startTime = System.currentTimeMillis();
		Log.info( () -> format("getUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null)
			return error(BAD_REQUEST);

//		User cacheUser = RedisJedisPool.getFromCache(REDIS_USERS + userId, User.class);
//		if (cacheUser != null) {
//			csvLogger.logToCSV("Get user with redis", System.currentTimeMillis() - startTime);
//			return cacheUser.getPwd().equals( pwd ) ? ok(cacheUser) : error(FORBIDDEN);
//		}
		var result = validatedUserOrError(SqlDB.getOne( userId, User.class), pwd);
		csvLogger.logToCSV("Get user without redis", System.currentTimeMillis() - startTime);
		return result ;
	}

	@Override
	public Result<User> getUserWithoutPwd(String userId) {
		Log.info( () -> format("getUser : userId = %s, pwd = *no pwd*\n", userId));

		if (userId == null)
			return error(BAD_REQUEST);

		return SqlDB.getOne( userId, User.class);
	}

	@Override
	public Result<User> updateUser(String userId, String pwd, User other) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("updateUser : userId = %s, pwd = %s, user: %s\n", userId, pwd, other));

		if (badUpdateUserInfo(userId, pwd, other))
			return error(BAD_REQUEST);

		Result<User> result = errorOrResult( validatedUserOrError(SqlDB.getOne( userId, User.class), pwd), user -> SqlDB.updateOne( user.updateFrom(other)));
		csvLogger.logToCSV("update user", System.currentTimeMillis() - startTime);

//		if (result.isOK()) {
//			RedisJedisPool.addToCache(REDIS_USERS + userId, result.value());
//		}

		return result;
	}

	@Override
	public Result<User> deleteUser(String userId, String pwd) {
		long startTime = System.currentTimeMillis();
		Log.info(() -> format("deleteUser : userId = %s, pwd = %s\n", userId, pwd));

		if (userId == null || pwd == null )
			return error(BAD_REQUEST);

		return errorOrResult( validatedUserOrError(SqlDB.getOne( userId, User.class), pwd), user -> {
			// Delete user shorts and related info asynchronously in a separate thread
			Executors.defaultThreadFactory().newThread( () -> {
				JavaShortsForSQL.getInstance().deleteAllShorts(userId, pwd, Token.get(userId));
			}).start();

			Result<User> result = SqlDB.deleteOne( user);
			csvLogger.logToCSV("delete user", System.currentTimeMillis() - startTime);

//			if (result.isOK()) {
//				RedisJedisPool.removeFromCache(REDIS_USERS + userId);
//			}
			return result;
		});
	}

	@Override
	public Result<List<User>> searchUsers(String pattern) {
		long startTime = System.currentTimeMillis();
		Log.info( () -> format("searchUsers : patterns = %s\n", pattern));

		var sqlQuery = format("SELECT * FROM AppUser u WHERE UPPER(u.id) LIKE '%%%s%%'", pattern.toUpperCase());
		var sqlResult = SqlDB.sql(sqlQuery, User.class)
				.stream()
				.map(User::copyWithoutPassword)
				.toList();

		csvLogger.logToCSV("search users", System.currentTimeMillis() - startTime);
//		Getting cached users is obsolete since we need to get all of them from database anyway
//		List<User> cacheUsers = RedisJedisPool.getByKeyPatternFromCache(REDIS_USERS + format("*%s*", pattern.toUpperCase()), User.class);
//		List<User> cacheUsersWithoutPwd = cacheUsers.stream().map(User::copyWithoutPassword).toList();

		return ok(sqlResult);
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
