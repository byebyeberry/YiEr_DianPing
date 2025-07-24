package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        // 1. 从Redis中查询商户类型列表
        String key = CACHE_SHOP_TYPE_KEY;
        String shopTypeJson = stringRedisTemplate.opsForValue().get(key);

        // 2. 判断商户类型列表是否存在
        if (shopTypeJson != null) {
            // 3. 存在，返回商户类型列表
            return Result.ok(JSONUtil.toList(shopTypeJson, ShopType.class));
        }

        // 4. 不存在，查询数据库
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();

        if (shopTypeList == null) {
            // 5. 不存在，返回错误
            return Result.fail("商户类型不存在");
        }

        // 6. 存在，保存商户类型列表到Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shopTypeList), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 7. 返回商户类型列表
        return Result.ok(shopTypeList);

    }
}
