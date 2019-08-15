package redis.clients.jedis.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.CuckooJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.tests.utils.JedisSentinelTestUtil;

public class CuckooJedisSentinelPoolTest {
  private static final String MASTER_NAME = "mymaster";

  protected static HostAndPort master = HostAndPortUtil.getRedisServers().get(2);
  protected static HostAndPort slave1 = HostAndPortUtil.getRedisServers().get(3);

  protected static HostAndPort sentinel1 = HostAndPortUtil.getSentinelServers().get(1);
  protected static HostAndPort sentinel2 = HostAndPortUtil.getSentinelServers().get(3);

  protected static CuckooJedis sentinelCuckooJedis1;
  protected static CuckooJedis sentinelCuckooJedis2;

  protected Set<String> sentinels = new HashSet<String>();

  @Before
  public void setUp() throws Exception {
    sentinels.add(sentinel1.toString());
    sentinels.add(sentinel2.toString());

    sentinelCuckooJedis1 = new CuckooJedis(sentinel1);
    sentinelCuckooJedis2 = new CuckooJedis(sentinel2);
  }
  
  @Test
  public void repeatedSentinelPoolInitialization() {

    for(int i=0; i<20 ; ++i) {
      GenericObjectPoolConfig config = new GenericObjectPoolConfig();

      JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, sentinels, config, 1000,
          "foobared", 2);
      pool.getResource().close();
      pool.destroy();
    }
  }
  

  @Test(expected = JedisConnectionException.class)
  public void initializeWithNotAvailableSentinelsShouldThrowException() {
    Set<String> wrongSentinels = new HashSet<String>();
    wrongSentinels.add(new HostAndPort("localhost", 65432).toString());
    wrongSentinels.add(new HostAndPort("localhost", 65431).toString());

    JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, wrongSentinels);
    pool.destroy();
  }

  @Test(expected = JedisException.class)
  public void initializeWithNotMonitoredMasterNameShouldThrowException() {
    final String wrongMasterName = "wrongMasterName";
    JedisSentinelPool pool = new JedisSentinelPool(wrongMasterName, sentinels);
    pool.destroy();
  }

  @Test
  public void checkCloseableConnections() throws Exception {
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();

    JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, sentinels, config, 1000,
        "foobared", 2);
    CuckooJedis cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.set("foo", "bar");
    assertEquals("bar", cuckooJedis.get("foo"));
    cuckooJedis.close();
    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test
  public void ensureSafeTwiceFailover() throws InterruptedException {
    JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, sentinels,
        new GenericObjectPoolConfig(), 1000, "foobared", 2);

    forceFailover(pool);
    // after failover sentinel needs a bit of time to stabilize before a new
    // failover
    Thread.sleep(100);
    forceFailover(pool);

    // you can test failover as much as possible
  }

  @Test
  public void returnResourceShouldResetState() {
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, sentinels, config, 1000,
        "foobared", 2);

    CuckooJedis cuckooJedis = pool.getResource();
    CuckooJedis cuckooJedis2 = null;

    try {
      cuckooJedis.set("hello", "cuckooJedis");
      Transaction t = cuckooJedis.multi();
      t.set("hello", "world");
      cuckooJedis.close();

      cuckooJedis2 = pool.getResource();

      assertTrue(cuckooJedis == cuckooJedis2);
      assertEquals("cuckooJedis", cuckooJedis2.get("hello"));
    } catch (JedisConnectionException e) {
      if (cuckooJedis2 != null) {
        cuckooJedis2 = null;
      }
    } finally {
      cuckooJedis2.close();

      pool.destroy();
    }
  }

  @Test
  public void checkResourceIsCloseable() {
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, sentinels, config, 1000,
        "foobared", 2);

    CuckooJedis cuckooJedis = pool.getResource();
    try {
      cuckooJedis.set("hello", "cuckooJedis");
    } finally {
      cuckooJedis.close();
    }

    CuckooJedis cuckooJedis2 = pool.getResource();
    try {
      assertEquals(cuckooJedis, cuckooJedis2);
    } finally {
      cuckooJedis2.close();
    }
  }

  @Test
  public void customClientName() {
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    JedisSentinelPool pool = new JedisSentinelPool(MASTER_NAME, sentinels, config, 1000,
        "foobared", 0, "my_shiny_client_name");

    CuckooJedis cuckooJedis = pool.getResource();

    try {
      assertEquals("my_shiny_client_name", cuckooJedis.clientGetname());
    } finally {
      cuckooJedis.close();
      pool.destroy();
    }

    assertTrue(pool.isClosed());
  }

  private void forceFailover(JedisSentinelPool pool) throws InterruptedException {
    HostAndPort oldMaster = pool.getCurrentHostMaster();

    // cuckooJedis connection should be master
    CuckooJedis beforeFailoverCuckooJedis = pool.getResource();
    assertEquals("PONG", beforeFailoverCuckooJedis.ping());

    waitForFailover(pool, oldMaster);

    CuckooJedis afterFailoverCuckooJedis = pool.getResource();
    assertEquals("PONG", afterFailoverCuckooJedis.ping());
    assertEquals("foobared", afterFailoverCuckooJedis.configGet("requirepass").get(1));
    assertEquals(2, afterFailoverCuckooJedis.getDB());

    // returning both connections to the pool should not throw
    beforeFailoverCuckooJedis.close();
    afterFailoverCuckooJedis.close();
  }

  private void waitForFailover(JedisSentinelPool pool, HostAndPort oldMaster)
      throws InterruptedException {
    HostAndPort newMaster = JedisSentinelTestUtil.waitForNewPromotedMaster(MASTER_NAME,
            sentinelCuckooJedis1, sentinelCuckooJedis2);

    waitForJedisSentinelPoolRecognizeNewMaster(pool, newMaster);
  }

  private void waitForJedisSentinelPoolRecognizeNewMaster(JedisSentinelPool pool,
      HostAndPort newMaster) throws InterruptedException {

    while (true) {
      HostAndPort currentHostMaster = pool.getCurrentHostMaster();

      if (newMaster.equals(currentHostMaster)) break;

      System.out.println("JedisSentinelPool's master is not yet changed, sleep...");

      Thread.sleep(100);
    }
  }

}