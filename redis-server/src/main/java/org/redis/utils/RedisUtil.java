package org.redis.utils;

import lombok.extern.slf4j.Slf4j;
import org.redis.utils.RedisUtil;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by admin on 2018/12/24.
 */
@Slf4j
public class RedisUtil {

    private ShardedJedisPool shardedJedisPool;

    /**
     * 设置表数据
     *
     * @param tableName redis表名
     * @param fieldName redis字段名称
     * @param value     redis values 值修改
     * @return 返回成功与否
     */
    public Long hset(String tableName, String fieldName, String value) {
        ShardedJedis shardedJedis = null;
        Long length = null;
        try {
            shardedJedis = getShardedJedis();
            length = shardedJedis.hset(tableName, fieldName, value);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return length;
    }


    /**
     * 设置表数据
     *
     * @param tableName redis表名
     * @param fieldName redis字段名称
     * @param value     redis values 值修改
     * @return 返回成功与否
     */
    public Long hsetAndExpire(String tableName, String fieldName, String value, int seconds) {
        ShardedJedis shardedJedis = null;
        Long length = null;
        try {
            shardedJedis = getShardedJedis();
            length = shardedJedis.hset(tableName, fieldName, value);
            shardedJedis.expire(tableName, seconds);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return length;
    }

    /**
     * 设置表数据
     *
     * @param key redis values 值修改
     * @return 返回成功与否
     */
    public Set<String> keys(String key) {
        ShardedJedis shardedJedis = null;
        Set<String> result = null;
        try {
            shardedJedis = getShardedJedis();
            result = shardedJedis.hkeys(key);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return result;
    }

    public String hget(String tableName, String key) {
        ShardedJedis shardedJedis = null;
        String result = null;
        try {
            shardedJedis = getShardedJedis();
            result = shardedJedis.hget(tableName, key);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return result;
    }


    /**
     * 从名称为key的list取值value
     *
     * @param key Key值
     */
    public String lpop(String key) {
        ShardedJedis shardedJedis = null;
        String value = null;
        try {
            shardedJedis = getShardedJedis();
            value = shardedJedis.lpop(key);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return value;
    }

    /**
     * 在名称为key的list尾添加一个值为value的元素
     *
     * @param key   key值
     * @param value 值
     */
    public void rpush(String key, String value) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            shardedJedis.rpush(key, value);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }

    }

    /**
     * 根据下表获取redis数据
     * @param key
     * @param startIndex
     * @param endIndex
     */
    public List<String> lRange(String key, long startIndex, long endIndex) {
        List<String> values = null ;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            values=  shardedJedis.lrange(key, startIndex,endIndex);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return values;

    }


    /**
     * 根据下表移除redis队列数据
     * @param key
     * @param startIndex
     * @param endIndex
     */
    public String lTrim(String key, long startIndex,long endIndex) {
        String value = null ;
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            value=  shardedJedis.ltrim(key, startIndex,endIndex);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return value;

    }

    /**
     * @param key     存储key
     * @param value   存储数据
     * @param seconds 有效时间 单位秒
     */
    public void set(String key, String value, int seconds) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = shardedJedisPool.getResource();
            shardedJedis.setex(key, seconds, value);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }

    }

    public boolean setNx(String key, String value, int seconds) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            Long result = shardedJedis.setnx(key, value);
            if (result == 1) {
                shardedJedis.expire(key, seconds);
                return true;
            }
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return false;
    }

    public boolean setNx(String key, String value, long milliseconds) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            Long result = shardedJedis.setnx(key, value);
            if (result == 1) {
                shardedJedis.pexpire(key, milliseconds);
                return true;
            }
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return false;
    }


    /**
     * 存储键值对
     *
     * @param key   存储数据key
     * @param value 存储数据
     */
    public void set(String key, String value) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            shardedJedis.set(key, value);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
    }

    /**
     * 根据存储key获取相应的存储value数据
     *
     * @param key 存储数据key
     * @return string
     */
    public String get(String key) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            return shardedJedis.get(key);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return null;
    }

    /**
     * 根据存储key删除相应的数据
     *
     * @param key 存储数据key
     */
    public void delete(String key) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            shardedJedis.del(key);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
    }

    /**
     * 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略
     *
     * @param key     存储数据key
     * @param value   是否添加元素成功
     * @param seconds 添加存在秒数
     * @return 是否成功添加
     */
    public boolean sadd(String key, String value, int seconds) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            shardedJedis.expire(key, seconds);
            Long result = shardedJedis.sadd(key, value);
            return result > 0;
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return false;
    }

    /**
     * 将 key 中储存的数字值增一
     *
     * @param key key值
     * @return 真实的数量
     */
    public Long incr(String key) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            return shardedJedis.incr(key);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return -1L;
    }

    /**
     * 将 key 中储存的数字值变成
     *
     * @param key key值
     * @return 真实的数量
     */
    public Long incrBy(String key, Long value) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            return shardedJedis.incrBy(key, value);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return -1L;
    }

    /**
     * 将 key 中储存的数字值增一
     *
     * @param key key值
     * @return 真实的数量
     */
    public Long incrAndExpire(String key, int secords) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            Long result = shardedJedis.incr(key);
            shardedJedis.expire(key, secords);
            return result;
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return -1L;
    }

    /**
     * 设置失效时间
     *
     * @param key     key值
     * @param seconds 失效时间 秒
     * @return
     */
    public Long expire(String key, int seconds) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            return shardedJedis.expire(key, seconds);
        } catch (JedisConnectionException jedisE) {
            log.error( "redis的连接出现问题了:", jedisE);
        } catch (Exception e) {
            log.error("redis的连接出现未知异常问题:", e);
        } finally {
            closeSharedJedis(shardedJedis);
        }
        return 0L;
    }

    public void batchInsert(Map<String, String> batchData, int secords) {
        ShardedJedis shardedJedis = null;
        try {
            shardedJedis = getShardedJedis();
            ShardedJedisPipeline shardedJedisPipeline = shardedJedis.pipelined();
            for (Map.Entry<String, String> entry : batchData.entrySet()) {
                shardedJedisPipeline.set(entry.getKey(), entry.getValue());
                shardedJedisPipeline.expire(entry.getKey(), secords);
            }
            shardedJedisPipeline.syncAndReturnAll();
        } finally {
            closeSharedJedis(shardedJedis);
        }
    }


    /**
     * 获取ShardedJedis连接
     *
     * @return redis 连接
     */
    public ShardedJedis getShardedJedis() {
        return shardedJedisPool.getResource();
    }

    /**
     * 将连接返回给连接池
     *
     * @param shardedJedis redis连接
     */
    public void closeSharedJedis(ShardedJedis shardedJedis) {
        if (null != shardedJedis) {
            shardedJedisPool.returnResourceObject(shardedJedis);
        }
    }

    public ShardedJedisPool getShardedJedisPool() {
        return shardedJedisPool;
    }

    public void setShardedJedisPool(ShardedJedisPool shardedJedisPool) {
        this.shardedJedisPool = shardedJedisPool;
    }
}
