# bloom-cuckoo-jedis

modifiy xetorthio/jedis to support RedisBloom/RedisBloom

xetorthio/jedis https://github.com/xetorthio/jedis
RedisBloom/RedisBloom https://github.com/RedisBloom/RedisBloom

for now support : bloom/cuckoo filiter

### example

```java
JedisPool jedisPool = new JedisPool();
Jedis jedis = jedisPool.getResource();
jedis.cfaddnx("test", "t1");
jedis.cfinsertnx("test", "t1", "t2");
jedis.cfexists("test", "t1");
jedis.cfdel("test", "t1");
jedis.cfexists("test", "t1");
```
