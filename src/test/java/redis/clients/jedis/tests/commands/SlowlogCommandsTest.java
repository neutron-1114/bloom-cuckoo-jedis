package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import redis.clients.jedis.util.Slowlog;

public class SlowlogCommandsTest extends CuckooJedisCommandTestBase {

  @Test
  public void slowlog() {
    // do something
    cuckooJedis.configSet("slowlog-log-slower-than", "0");
    cuckooJedis.set("foo", "bar");
    cuckooJedis.set("foo2", "bar2");

    List<Slowlog> reducedLog = cuckooJedis.slowlogGet(1);
    assertEquals(1, reducedLog.size());

    Slowlog log = reducedLog.get(0);
    assertTrue(log.getId() > 0);
    assertTrue(log.getTimeStamp() > 0);
    assertTrue(log.getExecutionTime() > 0);
    assertNotNull(log.getArgs());

    List<byte[]> breducedLog = cuckooJedis.slowlogGetBinary(1);
    assertEquals(1, breducedLog.size());

    List<Slowlog> log1 = cuckooJedis.slowlogGet();
    List<byte[]> blog1 = cuckooJedis.slowlogGetBinary();

    assertNotNull(log1);
    assertNotNull(blog1);

    long len1 = cuckooJedis.slowlogLen();

    cuckooJedis.slowlogReset();

    List<Slowlog> log2 = cuckooJedis.slowlogGet();
    List<byte[]> blog2 = cuckooJedis.slowlogGetBinary();
    long len2 = cuckooJedis.slowlogLen();

    assertTrue(len1 > len2);
    assertTrue(log1.size() > log2.size());
    assertTrue(blog1.size() > blog2.size());
  }
}