# bloom-cuckoo-jedis

modifiy the jedis to support bloom/cuckoo filter

### example

```java
JedisPool jedisPool = new JedisPool();
Jedis jedis = jedisPool.getResource();
jedis.cfaddnx("test", "t1");
jedis.cfinsertnx("test", "t1", "t2");
jedis.cfexists("test", "t1");
jedis.cfdel(key, x);
jedis.cfexists("test", "t1");
```
