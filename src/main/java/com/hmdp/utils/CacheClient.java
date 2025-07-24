package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
//        stringRedisTemplate.expire(key, time, unit);
    }

    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;

        // 1. 从redis查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断商户是否存在redis缓存
        if (StrUtil.isNotBlank(json)) {
            // 3. 存在，返回商户
            return JSONUtil.toBean(json, type);
        }

        // 判断命中的是否是空值
        if (json !=  null) {
            return null;
        }

        // 4. 不存在，查询数据库
        R r = dbFallback.apply(id);

        // 5. 不存在，返回错误
        if (r == null) {
            // 将空值保存到redis中，解决缓存穿透
            stringRedisTemplate.opsForValue().set(key, " ", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误
            return null;
        }

        // 6. 存在，保存商户到redis
        this.set(key, JSONUtil.toJsonStr(r), time, unit);

        // 7. 返回商户
        return r;

    }


    public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {

        String key = keyPrefix + id;

        // 1. 从redis查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断商户是否存在redis缓存
        if (StrUtil.isNotBlank(json)) {
            // 3. 存在，返回商户
            return JSONUtil.toBean(json, type);
        }

        // 判断命中的是否是空值
        if (json != null) {
            return null;
        }

        // 4. 实现缓存重建
        // 4.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;

        R r = null;
        try {
            boolean isLock = tryLock(lockKey);

            // 4.2 判断是否获取成功
            if (!isLock) {
                // 4.3 失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }

            // 4.4 成功，根据id查询数据库
            r = dbFallback.apply(id);

            // 5. 不存在，返回错误
            if (r == null) {
                // 将空值保存到redis中，解决缓存穿透
                this.set(key, " ", time,  unit);
                // 返回错误
                return null;
            }

            // 6. 存在，保存商户到redis
            this.set(key, JSONUtil.toJsonStr(r), time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {

            // 7. 释放互斥锁
            unlock(lockKey);
        }

        // 8. 返回商户
        return r;
    }

    private boolean tryLock(String lockKey) {

        return Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS));
    }

    private void unlock(String lockKey) {
        stringRedisTemplate.delete(lockKey);
    }



    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public <R, ID> R  queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {

        String key = keyPrefix + id;

        // 1. 从redis查询商户缓存
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断商户是否存在redis缓存
        if (StrUtil.isBlank(json)) {
            // 3. 不存在，直接返回
            return null;
        }

        // 4. 命中，需要把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);

        // 5. 判断是否过期
        if (redisData.getExpireTime() !=  null && redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            // 5.1 未过期，直接返回商户信息
            return r;
        }
        // 5.2 已过期，需要缓存重建
        // 6. 缓存重建
        // 6.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        // 6.2 判断是否获取成功
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            // 6.3 成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    this.setWithLogicExpire(key, dbFallback.apply(id), time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });

        }

        // 6.4 返回过期的商户信息
        return r;

    }

}
