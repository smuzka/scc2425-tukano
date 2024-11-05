package tukano.impl;


import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import utils.JSON;
import utils.PropsEnv;

public class RedisJedisPool {


    private static JedisPool instance;

    public synchronized static JedisPool getCachePool() {
        if (instance != null)
            return instance;

        var poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);

        String RedisHostname = System.getenv("REDIS_HOSTNAME");
        int REDIS_PORT = Integer.parseInt(System.getenv("REDIS_PORT"));
        int REDIS_TIMEOUT = 2000;
        String RedisKey = System.getenv("REDIS_KEY");
        boolean REDIS_USE_TLS = true;

        poolConfig.setBlockWhenExhausted(true);
        instance = new JedisPool(poolConfig, RedisHostname, REDIS_PORT, REDIS_TIMEOUT, RedisKey, REDIS_USE_TLS);
        return instance;
    }

    public synchronized static <T> String addToCache(String key, T value) {
        try (Jedis jedis = RedisJedisPool.getCachePool().getResource()) {
            return jedis.set(key, JSON.encode(value));
        }
    }

    public synchronized static <T> String addToCacheWithExpiry(String key, T value, int expiry) {
        try (Jedis jedis = RedisJedisPool.getCachePool().getResource()) {
            return jedis.setex(key, expiry, JSON.encode(value));
        }
    }

    public synchronized static <T> T getFromCache(String key, Class<T> clazz) {
        try (Jedis jedis = RedisJedisPool.getCachePool().getResource()) {
            var value = jedis.get(key);
            if (value != null)
                return JSON.decode(value, clazz);
            return null;
        }
    }

    public synchronized static <T> List<T> getByKeyPatternFromCache(String pattern, Class<T> clazz) {
        try (Jedis jedis = RedisJedisPool.getCachePool().getResource()) {
            Set<String> keys = jedis.keys(pattern);

            return getMultipleFromCache(keys, clazz);
        }
    }

    public synchronized static <T> List<T> getMultipleFromCache(Set<String> keys, Class<T> clazz) {
        if (keys.isEmpty()) {
            return List.of();
        }
        try (Jedis jedis = RedisJedisPool.getCachePool().getResource()) {
            List<String> values = jedis.mget(keys.toArray(new String[0]));
            return values.stream().map(value -> JSON.decode(value, clazz)).toList();
        }
    }

    public synchronized static void removeFromCache(String key) {
        try (Jedis jedis = RedisJedisPool.getCachePool().getResource()) {
            jedis.del(key);
        }
    }

    public synchronized static void removeMultipleFromCache(Set<String> keys) {
        try (Jedis jedis = RedisJedisPool.getCachePool().getResource()) {
            jedis.del(keys.toString());
        }
    }
}