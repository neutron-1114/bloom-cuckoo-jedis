package redis.clients.jedis.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.CuckooJedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.tests.commands.CuckooJedisCommandTestBase;
import redis.clients.jedis.util.SafeEncoder;

public class CuckooJedisTest extends CuckooJedisCommandTestBase {
  @Test
  public void useWithoutConnecting() {
    CuckooJedis cuckooJedis = new CuckooJedis("localhost");
    cuckooJedis.auth("foobared");
    cuckooJedis.dbSize();
  }

  @Test
  public void checkBinaryData() {
    byte[] bigdata = new byte[1777];
    for (int b = 0; b < bigdata.length; b++) {
      bigdata[b] = (byte) ((byte) b % 255);
    }
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("data", SafeEncoder.encode(bigdata));

    String status = cuckooJedis.hmset("foo", hash);
    assertEquals("OK", status);
    assertEquals(hash, cuckooJedis.hgetAll("foo"));
  }

  @Test
  public void connectWithShardInfo() {
    JedisShardInfo shardInfo = new JedisShardInfo("localhost", Protocol.DEFAULT_PORT);
    shardInfo.setPassword("foobared");
    CuckooJedis cuckooJedis = new CuckooJedis(shardInfo);
    cuckooJedis.get("foo");
  }

  @Test
  public void timeoutConnection() throws Exception {
    CuckooJedis cuckooJedis = new CuckooJedis("localhost", 6379, 15000);
    cuckooJedis.auth("foobared");
    String timeout = cuckooJedis.configGet("timeout").get(1);
    cuckooJedis.configSet("timeout", "1");
    Thread.sleep(2000);
    try {
      cuckooJedis.hmget("foobar", "foo");
      fail("Operation should throw JedisConnectionException");
    } catch(JedisConnectionException jce) {
      // expected
    }
    cuckooJedis.close();

    // reset config
    cuckooJedis = new CuckooJedis("localhost", 6379);
    cuckooJedis.auth("foobared");
    cuckooJedis.configSet("timeout", timeout);
    cuckooJedis.close();
  }

  @Test
  public void timeoutConnectionWithURI() throws Exception {
    CuckooJedis cuckooJedis = new CuckooJedis(new URI("redis://:foobared@localhost:6380/2"), 15000);
    String timeout = cuckooJedis.configGet("timeout").get(1);
    cuckooJedis.configSet("timeout", "1");
    Thread.sleep(2000);
    try {
      cuckooJedis.hmget("foobar", "foo");
      fail("Operation should throw JedisConnectionException");
    } catch(JedisConnectionException jce) {
      // expected
    }
    cuckooJedis.close();

    // reset config
    cuckooJedis = new CuckooJedis(new URI("redis://:foobared@localhost:6380/2"));
    cuckooJedis.configSet("timeout", timeout);
    cuckooJedis.close();
  }

  @Test(expected = JedisDataException.class)
  public void failWhenSendingNullValues() {
    cuckooJedis.set("foo", null);
  }

  @Test(expected = InvalidURIException.class)
  public void shouldThrowInvalidURIExceptionForInvalidURI() throws URISyntaxException {
    CuckooJedis j = new CuckooJedis(new URI("localhost:6380"));
    j.ping();
  }

  @Test
  public void shouldReconnectToSameDB() throws IOException {
    cuckooJedis.select(1);
    cuckooJedis.set("foo", "bar");
    cuckooJedis.getClient().getSocket().shutdownInput();
    cuckooJedis.getClient().getSocket().shutdownOutput();
    assertEquals("bar", cuckooJedis.get("foo"));
  }

  @Test
  public void startWithUrlString() {
    CuckooJedis j = new CuckooJedis("localhost", 6380);
    j.auth("foobared");
    j.select(2);
    j.set("foo", "bar");
    CuckooJedis cuckooJedis = new CuckooJedis("redis://:foobared@localhost:6380/2");
    assertEquals("PONG", cuckooJedis.ping());
    assertEquals("bar", cuckooJedis.get("foo"));
  }

  @Test
  public void startWithUrl() throws URISyntaxException {
    CuckooJedis j = new CuckooJedis("localhost", 6380);
    j.auth("foobared");
    j.select(2);
    j.set("foo", "bar");
    CuckooJedis cuckooJedis = new CuckooJedis(new URI("redis://:foobared@localhost:6380/2"));
    assertEquals("PONG", cuckooJedis.ping());
    assertEquals("bar", cuckooJedis.get("foo"));
  }

  @Test
  public void shouldNotUpdateDbIndexIfSelectFails() throws URISyntaxException {
    int currentDb = cuckooJedis.getDB();
    try {
      int invalidDb = -1;
      cuckooJedis.select(invalidDb);

      fail("Should throw an exception if tried to select invalid db");
    } catch (JedisException e) {
      assertEquals(currentDb, cuckooJedis.getDB());
    }
  }

  @Test
  public void allowUrlWithNoDBAndNoPassword() {
    CuckooJedis cuckooJedis = new CuckooJedis("redis://localhost:6380");
    cuckooJedis.auth("foobared");
    assertEquals("localhost", cuckooJedis.getClient().getHost());
    assertEquals(6380, cuckooJedis.getClient().getPort());
    assertEquals(0, cuckooJedis.getDB());

    cuckooJedis = new CuckooJedis("redis://localhost:6380/");
    cuckooJedis.auth("foobared");
    assertEquals("localhost", cuckooJedis.getClient().getHost());
    assertEquals(6380, cuckooJedis.getClient().getPort());
    assertEquals(0, cuckooJedis.getDB());
  }

  @Test
  public void checkCloseable() {
    cuckooJedis.close();
    BinaryJedis bj = new BinaryJedis("localhost");
    bj.connect();
    bj.close();
  }

  @Test
  public void checkDisconnectOnQuit() {
    cuckooJedis.quit();
    assertFalse(cuckooJedis.getClient().isConnected());
  }

}