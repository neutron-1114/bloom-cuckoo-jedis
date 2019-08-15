package redis.clients.jedis.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;

import redis.clients.jedis.*;
import redis.clients.jedis.CuckooJedis;
import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisExhaustedPoolException;

public class CuckooCuckooJedisPoolTest {
  private static HostAndPort hnp = HostAndPortUtil.getRedisServers().get(0);

  @Test
  public void checkConnections() {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000);
    CuckooJedis cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.set("foo", "bar");
    assertEquals("bar", cuckooJedis.get("foo"));
    cuckooJedis.close();
    pool.destroy();
    assertTrue(pool.isClosed());
  }

  @Test
  public void checkCloseableConnections() throws Exception {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000);
    CuckooJedis cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.set("foo", "bar");
    assertEquals("bar", cuckooJedis.get("foo"));
    cuckooJedis.close();
    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test
  public void checkConnectionWithDefaultPort() {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort());
    CuckooJedis cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.set("foo", "bar");
    assertEquals("bar", cuckooJedis.get("foo"));
    cuckooJedis.close();
    pool.destroy();
    assertTrue(pool.isClosed());
  }

  @Test
  public void checkJedisIsReusedWhenReturned() {

    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort());
    CuckooJedis cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.set("foo", "0");
    cuckooJedis.close();

    cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.incr("foo");
    cuckooJedis.close();
    pool.destroy();
    assertTrue(pool.isClosed());
  }

  @Test
  public void checkPoolRepairedWhenJedisIsBroken() {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort());
    CuckooJedis cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.quit();
    cuckooJedis.close();

    cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.incr("foo");
    cuckooJedis.close();
    pool.destroy();
    assertTrue(pool.isClosed());
  }

  @Test(expected = JedisExhaustedPoolException.class)
  public void checkPoolOverflow() {
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    CuckooJedisPool pool = new CuckooJedisPool(config, hnp.getHost(), hnp.getPort());
    CuckooJedis cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.set("foo", "0");

    CuckooJedis newCuckooJedis = pool.getResource();
    newCuckooJedis.auth("foobared");
    newCuckooJedis.incr("foo");
  }

  @Test
  public void securePool() {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setTestOnBorrow(true);
    CuckooJedisPool pool = new CuckooJedisPool(config, hnp.getHost(), hnp.getPort(), 2000, "foobared");
    CuckooJedis cuckooJedis = pool.getResource();
    cuckooJedis.set("foo", "bar");
    cuckooJedis.close();
    pool.destroy();
    assertTrue(pool.isClosed());
  }

  @Test
  public void nonDefaultDatabase() {
    CuckooJedisPool pool0 = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000,
        "foobared");
    CuckooJedis cuckooJedis0 = pool0.getResource();
    cuckooJedis0.set("foo", "bar");
    assertEquals("bar", cuckooJedis0.get("foo"));
    cuckooJedis0.close();
    pool0.destroy();
    assertTrue(pool0.isClosed());

    CuckooJedisPool pool1 = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000,
        "foobared", 1);
    CuckooJedis cuckooJedis1 = pool1.getResource();
    assertNull(cuckooJedis1.get("foo"));
    cuckooJedis1.close();
    pool1.destroy();
    assertTrue(pool1.isClosed());
  }

  @Test
  public void startWithUrlString() {
    CuckooJedis j = new CuckooJedis("localhost", 6380);
    j.auth("foobared");
    j.select(2);
    j.set("foo", "bar");
    CuckooJedisPool pool = new CuckooJedisPool("redis://:foobared@localhost:6380/2");
    CuckooJedis cuckooJedis = pool.getResource();
    assertEquals("PONG", cuckooJedis.ping());
    assertEquals("bar", cuckooJedis.get("foo"));
  }

  @Test
  public void startWithUrl() throws URISyntaxException {
    CuckooJedis j = new CuckooJedis("localhost", 6380);
    j.auth("foobared");
    j.select(2);
    j.set("foo", "bar");
    CuckooJedisPool pool = new CuckooJedisPool(new URI("redis://:foobared@localhost:6380/2"));
    CuckooJedis cuckooJedis = pool.getResource();
    assertEquals("PONG", cuckooJedis.ping());
    assertEquals("bar", cuckooJedis.get("foo"));
  }

  @Test(expected = InvalidURIException.class)
  public void shouldThrowInvalidURIExceptionForInvalidURI() throws URISyntaxException {
    CuckooJedisPool pool = new CuckooJedisPool(new URI("localhost:6380"));
  }

  @Test
  public void allowUrlWithNoDBAndNoPassword() throws URISyntaxException {
    new CuckooJedisPool("redis://localhost:6380");
    new CuckooJedisPool(new URI("redis://localhost:6380"));
  }

  @Test
  public void selectDatabaseOnActivation() {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000,
        "foobared");

    CuckooJedis cuckooJedis0 = pool.getResource();
    assertEquals(0, cuckooJedis0.getDB());

    cuckooJedis0.select(1);
    assertEquals(1, cuckooJedis0.getDB());

    cuckooJedis0.close();

    CuckooJedis cuckooJedis1 = pool.getResource();
    assertTrue("CuckooJedis instance was not reused", cuckooJedis1 == cuckooJedis0);
    assertEquals(0, cuckooJedis1.getDB());

    cuckooJedis1.close();
    pool.destroy();
    assertTrue(pool.isClosed());
  }

  @Test
  public void customClientName() {
    CuckooJedisPool pool0 = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000,
        "foobared", 0, "my_shiny_client_name");

    CuckooJedis cuckooJedis = pool0.getResource();

    assertEquals("my_shiny_client_name", cuckooJedis.clientGetname());

    cuckooJedis.close();
    pool0.destroy();
    assertTrue(pool0.isClosed());
  }

  @Test
  public void returnResourceDestroysResourceOnException() {

    class CrashingCuckooJedis extends CuckooJedis {
      @Override
      public void resetState() {
        throw new RuntimeException();
      }
    }

    final AtomicInteger destroyed = new AtomicInteger(0);

    class CrashingJedisPooledObjectFactory implements PooledObjectFactory<CuckooJedis> {

      @Override
      public PooledObject<CuckooJedis> makeObject() throws Exception {
        return new DefaultPooledObject<CuckooJedis>(new CrashingCuckooJedis());
      }

      @Override
      public void destroyObject(PooledObject<CuckooJedis> p) throws Exception {
        destroyed.incrementAndGet();
      }

      @Override
      public boolean validateObject(PooledObject<CuckooJedis> p) {
        return true;
      }

      @Override
      public void activateObject(PooledObject<CuckooJedis> p) throws Exception {
      }

      @Override
      public void passivateObject(PooledObject<CuckooJedis> p) throws Exception {
      }
    }

    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    CuckooJedisPool pool = new CuckooJedisPool(config, hnp.getHost(), hnp.getPort(), 2000, "foobared");
    pool.initPool(config, new CrashingJedisPooledObjectFactory());
    CuckooJedis crashingCuckooJedis = pool.getResource();

    try {
      crashingCuckooJedis.close();
    } catch (Exception ignored) {
    }

    assertEquals(1, destroyed.get());
  }

  @Test
  public void returnResourceShouldResetState() {
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    CuckooJedisPool pool = new CuckooJedisPool(config, hnp.getHost(), hnp.getPort(), 2000, "foobared");

    CuckooJedis cuckooJedis = pool.getResource();
    try {
      cuckooJedis.set("hello", "cuckooJedis");
      Transaction t = cuckooJedis.multi();
      t.set("hello", "world");
    } finally {
      cuckooJedis.close();
    }

    CuckooJedis cuckooJedis2 = pool.getResource();
    try {
      assertTrue(cuckooJedis == cuckooJedis2);
      assertEquals("cuckooJedis", cuckooJedis2.get("hello"));
    } finally {
      cuckooJedis2.close();
    }

    pool.destroy();
    assertTrue(pool.isClosed());
  }

  @Test
  public void checkResourceIsCloseable() {
    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
    config.setMaxTotal(1);
    config.setBlockWhenExhausted(false);
    CuckooJedisPool pool = new CuckooJedisPool(config, hnp.getHost(), hnp.getPort(), 2000, "foobared");

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
  public void getNumActiveIsNegativeWhenPoolIsClosed() {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000,
        "foobared", 0, "my_shiny_client_name");

    pool.destroy();
    assertTrue(pool.getNumActive() < 0);
  }

  @Test
  public void getNumActiveReturnsTheCorrectNumber() {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000);
    CuckooJedis cuckooJedis = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.set("foo", "bar");
    assertEquals("bar", cuckooJedis.get("foo"));

    assertEquals(1, pool.getNumActive());

    CuckooJedis cuckooJedis2 = pool.getResource();
    cuckooJedis.auth("foobared");
    cuckooJedis.set("foo", "bar");

    assertEquals(2, pool.getNumActive());

    cuckooJedis.close();
    assertEquals(1, pool.getNumActive());

    cuckooJedis2.close();

    assertEquals(0, pool.getNumActive());

    pool.destroy();
  }

  @Test
  public void testAddObject() {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000);
    pool.addObjects(1);
    assertEquals(1, pool.getNumIdle());
    pool.destroy();
  }

  @Test
  public void closeResourceTwice() {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000);
    CuckooJedis j = pool.getResource();
    j.auth("foobared");
    j.ping();
    j.close();
    j.close();
  }

  @Test
  public void closeBrokenResourceTwice() {
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000);
    CuckooJedis j = pool.getResource();
    try {
      // make connection broken
      j.getClient().getOne();
      fail();
    } catch (Exception e) {
    }
    assertTrue(j.getClient().isBroken());
    j.close();
    j.close();
  }

  @Test
  public void testCloseConnectionOnMakeObject() {
    JedisPoolConfig config = new JedisPoolConfig();
    config.setTestOnBorrow(true);
    CuckooJedisPool pool = new CuckooJedisPool(new JedisPoolConfig(), hnp.getHost(), hnp.getPort(), 2000,
        "wrong pass");
    CuckooJedis cuckooJedis = new CuckooJedis("redis://:foobared@localhost:6379/");
    int currentClientCount = getClientCount(cuckooJedis.clientList());
    try {
      pool.getResource();
      fail("Should throw exception as password is incorrect.");
    } catch (Exception e) {
      assertEquals(currentClientCount, getClientCount(cuckooJedis.clientList()));
    }

  }

  private int getClientCount(final String clientList) {
    return clientList.split("\n").length;
  }

}
