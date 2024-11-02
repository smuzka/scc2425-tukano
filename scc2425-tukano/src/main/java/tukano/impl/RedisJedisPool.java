package tukano.impl;


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

        String RedisHostname = PropsEnv.get("REDIS_HOSTNAME", "");
        int REDIS_PORT = Integer.parseInt(PropsEnv.get("REDIS_PORT", "6380"));
        int REDIS_TIMEOUT = 2000;
        String RedisKey = PropsEnv.get("REDIS_KEY", "");
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

    public synchronized static <T> T getFromCache(String key, Class<T> clazz) {
        try (Jedis jedis = RedisJedisPool.getCachePool().getResource()) {
            var value = jedis.get(key);
            if (value != null)
                return JSON.decode(value, clazz);
            return null;
        }
    }

    public synchronized static void removeFromCache(String key) {
        try (Jedis jedis = RedisJedisPool.getCachePool().getResource()) {
            jedis.del(key);
        }
    }
}