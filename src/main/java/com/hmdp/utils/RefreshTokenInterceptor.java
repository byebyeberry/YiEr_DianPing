package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        // 1. 获取请求头中的token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            return false;
        }

        // 2. 基于token获取redis中的用户
        Map<Object, Object> userMAP = stringRedisTemplate.opsForHash().entries(LOGIN_USER_KEY + token);


//        // 3. 判断用户是否存在
//        if (userMAP.isEmpty()) {
//            // 4. 不存在，拦截，返回401
//            response.setStatus(401);
//            return false;
//        }


        // 5. 存在，将查询到的hash数据转为UserDTO
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMAP, new UserDTO(), false);


        // 6. 存在，保存用户信息到ThreadLocal
        UserHolder.saveUser(userDTO);

        // 7. 刷新token有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY + token, LOGIN_USER_TTL, TimeUnit.MINUTES);

        // 8. 放行
        return true;


        //


        //        // 1. 获取session
//        HttpSession session = request.getSession();
//
//        // 2. 获取session中的用户
//        Object user = (User)session.getAttribute("user");
//        UserDTO userDTO = new UserDTO();
//        BeanUtil.copyProperties(user, userDTO);
//
//        // 3. 判断用户是否存在
//        if (user == null) {
//            // 4. 不存在，拦截，返回401
//            response.setStatus(401);
//            return false;
//        }
//
//        // 5. 存在，保存用户信息到ThreadLocal
//        UserHolder.saveUser(userDTO);
//
//        // 6. 放行
//
//        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 移除用户，避免内存泄漏
        UserHolder.removeUser();
    }

}
