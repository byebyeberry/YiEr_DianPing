package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {

        // 缓存穿透
        // Shop shop = queryWithPassThrough(id);

        // 互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
        Shop shop = queryWithLogicalExpire(id);

        if (shop == null) {
            return Result.fail("商户不存在");
        }

        // 7. 返回商户
        return Result.ok(shop);

    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    private Shop queryWithLogicalExpire(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;

        // 1. 从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断商户是否存在redis缓存
        if (StrUtil.isBlank(shopJson)) {
            // 3. 不存在，直接返回
            return null;
        }

        // 4. 命中，需要把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);

        // 5. 判断是否过期
        if (redisData.getExpireTime() !=  null && redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            // 5.1 未过期，直接返回商户信息
            return shop;
        }
        // 5.2 已过期，需要缓存重建
        // 6. 缓存重建
        // 6.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        // 6.2 判断是否获取成功
        boolean isLock = tryLock(lockKey);
        if (isLock) {
            // TODO 6.3 成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    this.saveShop2Redis(id, 20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });

        }

        // 6.4 返回过期的商户信息
        return shop;

    }

    private Shop queryWithMutex(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;

        // 1. 从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断商户是否存在redis缓存
        if (StrUtil.isNotBlank(shopJson)) {
            // 3. 存在，返回商户
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // 判断命中的是否是空值
        if (shopJson != null) {
            return null;
        }

        // 4. 实现缓存重建
        // 4.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;

        Shop shop;
        try {
            boolean isLock = tryLock(lockKey);

            // 4.2 判断是否获取成功
            if (!isLock) {
                // 4.3 失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            // 4.4 成功，根据id查询数据库
            shop = getById(id);

            // 5. 不存在，返回错误
            if (shop == null) {
                // 将空值保存到redis中，解决缓存穿透
                stringRedisTemplate.opsForValue().set(key, " ", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回错误
                return null;
            }

            // 6. 存在，保存商户到redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {

            // 7. 释放互斥锁
            unlock(lockKey);
        }

        // 8. 返回商户
        return shop;
    }

    private boolean tryLock(String lockKey) {

        return Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS));
    }

    private void unlock(String lockKey) {
        stringRedisTemplate.delete(lockKey);
    }

    private void saveShop2Redis(Long id, Long expireSeconds) {
        // 1. 查询商户数据
        Shop shop = getById(id);

        // 2. 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        // 3. 保存商户数据到redis
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));

    }

    //将上面的缓存穿透函数封装一下
    private Shop queryWithPassThrough(Long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;

        // 1. 从redis查询商户缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断商户是否存在redis缓存
        if (StrUtil.isNotBlank(shopJson)) {
            // 3. 存在，返回商户
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        // 判断命中的是否是空值
        if (shopJson !=  null) {
            return null;
        }

        // 4. 不存在，查询数据库
        Shop shop = getById(id);

        // 5. 不存在，返回错误
        if (shop == null) {
            // 将空值保存到redis中，解决缓存穿透
            stringRedisTemplate.opsForValue().set(key, " ", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误
            return null;
        }

        // 6. 存在，保存商户到redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop));

        // 7. 返回商户
        return shop;

    }


    @Override
    @Transactional
    public Result update(Shop shop) {

        Long id = shop.getId();
        if (id == null) {
            return Result.fail("商户ID不能为空");
        }

        // 1. 更新数据库
        updateById(shop);
        // 2. 删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + shop.getId());

        return Result.ok();

    }
}
