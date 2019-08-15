package redis.clients.jedis;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.util.Pool;

public class JedisPoolAbstract extends Pool<CuckooJedis> {

  public JedisPoolAbstract() {
    super();
  }

  public JedisPoolAbstract(GenericObjectPoolConfig poolConfig, PooledObjectFactory<CuckooJedis> factory) {
    super(poolConfig, factory);
  }

  @Override
  protected void returnBrokenResource(CuckooJedis resource) {
    super.returnBrokenResource(resource);
  }

  @Override
  protected void returnResource(CuckooJedis resource) {
    super.returnResource(resource);
  }
}
