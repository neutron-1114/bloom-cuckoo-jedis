package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import redis.clients.jedis.util.SafeEncoder;

public class ObjectCommandsTest extends CuckooJedisCommandTestBase {

  private String key = "mylist";
  private byte[] binaryKey = SafeEncoder.encode(key);

  @Test
  public void objectRefcount() {
    cuckooJedis.lpush(key, "hello world");
    Long refcount = cuckooJedis.objectRefcount(key);
    assertEquals(new Long(1), refcount);

    // Binary
    refcount = cuckooJedis.objectRefcount(binaryKey);
    assertEquals(new Long(1), refcount);

  }

  @Test
  public void objectEncoding() {
    cuckooJedis.lpush(key, "hello world");
    String encoding = cuckooJedis.objectEncoding(key);
    assertEquals("quicklist", encoding);

    // Binary
    encoding = SafeEncoder.encode(cuckooJedis.objectEncoding(binaryKey));
    assertEquals("quicklist", encoding);
  }

  @Test
  public void objectIdletime() throws InterruptedException {
    cuckooJedis.lpush(key, "hello world");

    Long time = cuckooJedis.objectIdletime(key);
    assertEquals(new Long(0), time);

    // Binary
    time = cuckooJedis.objectIdletime(binaryKey);
    assertEquals(new Long(0), time);
  }
}