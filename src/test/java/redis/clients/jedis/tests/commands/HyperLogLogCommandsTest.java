package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import redis.clients.jedis.util.SafeEncoder;

public class HyperLogLogCommandsTest extends CuckooJedisCommandTestBase {

  @Test
  public void pfadd() {
    long status = cuckooJedis.pfadd("foo", "a");
    assertEquals(1, status);

    status = cuckooJedis.pfadd("foo", "a");
    assertEquals(0, status);
  }

  @Test
  public void pfaddBinary() {
    byte[] bFoo = SafeEncoder.encode("foo");
    byte[] bBar = SafeEncoder.encode("bar");
    byte[] bBar2 = SafeEncoder.encode("bar2");

    long status = cuckooJedis.pfadd(bFoo, bBar, bBar2);
    assertEquals(1, status);

    status = cuckooJedis.pfadd(bFoo, bBar, bBar2);
    assertEquals(0, status);
  }

  @Test
  public void pfcount() {
    long status = cuckooJedis.pfadd("hll", "foo", "bar", "zap");
    assertEquals(1, status);

    status = cuckooJedis.pfadd("hll", "zap", "zap", "zap");
    assertEquals(0, status);

    status = cuckooJedis.pfadd("hll", "foo", "bar");
    assertEquals(0, status);

    status = cuckooJedis.pfcount("hll");
    assertEquals(3, status);
  }

  @Test
  public void pfcounts() {
    long status = cuckooJedis.pfadd("hll_1", "foo", "bar", "zap");
    assertEquals(1, status);
    status = cuckooJedis.pfadd("hll_2", "foo", "bar", "zap");
    assertEquals(1, status);

    status = cuckooJedis.pfadd("hll_3", "foo", "bar", "baz");
    assertEquals(1, status);
    status = cuckooJedis.pfcount("hll_1");
    assertEquals(3, status);
    status = cuckooJedis.pfcount("hll_2");
    assertEquals(3, status);
    status = cuckooJedis.pfcount("hll_3");
    assertEquals(3, status);

    status = cuckooJedis.pfcount("hll_1", "hll_2");
    assertEquals(3, status);

    status = cuckooJedis.pfcount("hll_1", "hll_2", "hll_3");
    assertEquals(4, status);

  }

  @Test
  public void pfcountBinary() {
    byte[] bHll = SafeEncoder.encode("hll");
    byte[] bFoo = SafeEncoder.encode("foo");
    byte[] bBar = SafeEncoder.encode("bar");
    byte[] bZap = SafeEncoder.encode("zap");

    long status = cuckooJedis.pfadd(bHll, bFoo, bBar, bZap);
    assertEquals(1, status);

    status = cuckooJedis.pfadd(bHll, bZap, bZap, bZap);
    assertEquals(0, status);

    status = cuckooJedis.pfadd(bHll, bFoo, bBar);
    assertEquals(0, status);

    status = cuckooJedis.pfcount(bHll);
    assertEquals(3, status);
  }

  @Test
  public void pfmerge() {
    long status = cuckooJedis.pfadd("hll1", "foo", "bar", "zap", "a");
    assertEquals(1, status);

    status = cuckooJedis.pfadd("hll2", "a", "b", "c", "foo");
    assertEquals(1, status);

    String mergeStatus = cuckooJedis.pfmerge("hll3", "hll1", "hll2");
    assertEquals("OK", mergeStatus);

    status = cuckooJedis.pfcount("hll3");
    assertEquals(6, status);
  }

  @Test
  public void pfmergeBinary() {
    byte[] bHll1 = SafeEncoder.encode("hll1");
    byte[] bHll2 = SafeEncoder.encode("hll2");
    byte[] bHll3 = SafeEncoder.encode("hll3");
    byte[] bFoo = SafeEncoder.encode("foo");
    byte[] bBar = SafeEncoder.encode("bar");
    byte[] bZap = SafeEncoder.encode("zap");
    byte[] bA = SafeEncoder.encode("a");
    byte[] bB = SafeEncoder.encode("b");
    byte[] bC = SafeEncoder.encode("c");

    long status = cuckooJedis.pfadd(bHll1, bFoo, bBar, bZap, bA);
    assertEquals(1, status);

    status = cuckooJedis.pfadd(bHll2, bA, bB, bC, bFoo);
    assertEquals(1, status);

    String mergeStatus = cuckooJedis.pfmerge(bHll3, bHll1, bHll2);
    assertEquals("OK", mergeStatus);

    status = cuckooJedis.pfcount("hll3");
    assertEquals(6, status);
  }
}
