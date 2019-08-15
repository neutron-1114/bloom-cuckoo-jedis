package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertArrayEquals;

import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;

import redis.clients.jedis.CuckooJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.tests.HostAndPortUtil;

public abstract class CuckooJedisCommandTestBase {
  protected static final HostAndPort hnp = HostAndPortUtil.getRedisServers().get(0);

  protected CuckooJedis cuckooJedis;

  public CuckooJedisCommandTestBase() {
    super();
  }

  @Before
  public void setUp() throws Exception {
    cuckooJedis = new CuckooJedis(hnp.getHost(), hnp.getPort(), 500);
    cuckooJedis.connect();
    cuckooJedis.auth("foobared");
    cuckooJedis.flushAll();
  }

  @After
  public void tearDown() {
    cuckooJedis.disconnect();
  }

  protected CuckooJedis createJedis() {
    CuckooJedis j = new CuckooJedis(hnp);
    j.connect();
    j.auth("foobared");
    j.flushAll();
    return j;
  }

  protected boolean arrayContains(List<byte[]> array, byte[] expected) {
    for (byte[] a : array) {
      try {
        assertArrayEquals(a, expected);
        return true;
      } catch (AssertionError e) {

      }
    }
    return false;
  }

  protected boolean setContains(Set<byte[]> set, byte[] expected) {
    for (byte[] a : set) {
      try {
        assertArrayEquals(a, expected);
        return true;
      } catch (AssertionError e) {

      }
    }
    return false;
  }
}
