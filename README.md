# bloom-cuckoo-jedis

modifiy xetorthio/jedis to support bloom/cuckoo filter

xetorthio/jedis https://github.com/xetorthio/jedis

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
