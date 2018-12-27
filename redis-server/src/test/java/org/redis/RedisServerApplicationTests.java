package org.redis;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.redis.utils.RedisTemplateUtil;
import org.redis.utils.RedisUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class RedisServerApplicationTests {

	@Autowired
	RedisUtil redisUtil;

	@Autowired(required = false)
	RedisTemplateUtil redisTemplateUtil;

	@Test
	public void contextLoads() {

		redisUtil.set("redisUtil","redisUtil");

		redisTemplateUtil.set("redisTemplateUtil","redisTemplateUtil");

		log.info("----redisUtil value is : {}----",redisUtil.get("redisUtil"));

		log.info("----redisTemplateUtil value is : {}----",null == redisTemplateUtil.get("redisTemplateUtil") ? "":redisTemplateUtil.get("redisTemplateUtil").toString() );
	}

}

