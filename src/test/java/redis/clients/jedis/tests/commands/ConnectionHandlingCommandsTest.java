package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.CuckooJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.tests.HostAndPortUtil;

public class ConnectionHandlingCommandsTest {
  private static HostAndPort hnp = HostAndPortUtil.getRedisServers().get(0);

  @Test
  public void quit() {
    CuckooJedis cuckooJedis = new CuckooJedis(hnp);
    assertEquals("OK", cuckooJedis.quit());
  }

  @Test
  public void binary_quit() {
    BinaryJedis bj = new BinaryJedis(hnp);
    assertEquals("OK", bj.quit());
  }
}