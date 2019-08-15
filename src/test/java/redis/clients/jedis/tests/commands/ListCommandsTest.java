package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static redis.clients.jedis.tests.utils.AssertUtil.assertByteArrayListEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import redis.clients.jedis.CuckooJedis;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.exceptions.JedisDataException;

public class ListCommandsTest extends CuckooJedisCommandTestBase {
  final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
  final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
  final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };
  final byte[] bA = { 0x0A };
  final byte[] bB = { 0x0B };
  final byte[] bC = { 0x0C };
  final byte[] b1 = { 0x01 };
  final byte[] b2 = { 0x02 };
  final byte[] b3 = { 0x03 };
  final byte[] bhello = { 0x04, 0x02 };
  final byte[] bx = { 0x02, 0x04 };
  final byte[] bdst = { 0x11, 0x12, 0x13, 0x14 };

  @Test
  public void rpush() {
    long size = cuckooJedis.rpush("foo", "bar");
    assertEquals(1, size);
    size = cuckooJedis.rpush("foo", "foo");
    assertEquals(2, size);
    size = cuckooJedis.rpush("foo", "bar", "foo");
    assertEquals(4, size);

    // Binary
    long bsize = cuckooJedis.rpush(bfoo, bbar);
    assertEquals(1, bsize);
    bsize = cuckooJedis.rpush(bfoo, bfoo);
    assertEquals(2, bsize);
    bsize = cuckooJedis.rpush(bfoo, bbar, bfoo);
    assertEquals(4, bsize);

  }

  @Test
  public void lpush() {
    long size = cuckooJedis.lpush("foo", "bar");
    assertEquals(1, size);
    size = cuckooJedis.lpush("foo", "foo");
    assertEquals(2, size);
    size = cuckooJedis.lpush("foo", "bar", "foo");
    assertEquals(4, size);

    // Binary
    long bsize = cuckooJedis.lpush(bfoo, bbar);
    assertEquals(1, bsize);
    bsize = cuckooJedis.lpush(bfoo, bfoo);
    assertEquals(2, bsize);
    bsize = cuckooJedis.lpush(bfoo, bbar, bfoo);
    assertEquals(4, bsize);

  }

  @Test
  public void llen() {
    assertEquals(0, cuckooJedis.llen("foo").intValue());
    cuckooJedis.lpush("foo", "bar");
    cuckooJedis.lpush("foo", "car");
    assertEquals(2, cuckooJedis.llen("foo").intValue());

    // Binary
    assertEquals(0, cuckooJedis.llen(bfoo).intValue());
    cuckooJedis.lpush(bfoo, bbar);
    cuckooJedis.lpush(bfoo, bcar);
    assertEquals(2, cuckooJedis.llen(bfoo).intValue());

  }

  @Test
  public void llenNotOnList() {
    try {
      cuckooJedis.set("foo", "bar");
      cuckooJedis.llen("foo");
      fail("JedisDataException expected");
    } catch (final JedisDataException e) {
    }

    // Binary
    try {
      cuckooJedis.set(bfoo, bbar);
      cuckooJedis.llen(bfoo);
      fail("JedisDataException expected");
    } catch (final JedisDataException e) {
    }

  }

  @Test
  public void lrange() {
    cuckooJedis.rpush("foo", "a");
    cuckooJedis.rpush("foo", "b");
    cuckooJedis.rpush("foo", "c");

    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");

    List<String> range = cuckooJedis.lrange("foo", 0, 2);
    assertEquals(expected, range);

    range = cuckooJedis.lrange("foo", 0, 20);
    assertEquals(expected, range);

    expected = new ArrayList<String>();
    expected.add("b");
    expected.add("c");

    range = cuckooJedis.lrange("foo", 1, 2);
    assertEquals(expected, range);

    assertNull(cuckooJedis.lrange("foo", 2, 1));

    // Binary
    cuckooJedis.rpush(bfoo, bA);
    cuckooJedis.rpush(bfoo, bB);
    cuckooJedis.rpush(bfoo, bC);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);
    bexpected.add(bC);

    List<byte[]> brange = cuckooJedis.lrange(bfoo, 0, 2);
    assertByteArrayListEquals(bexpected, brange);

    brange = cuckooJedis.lrange(bfoo, 0, 20);
    assertByteArrayListEquals(bexpected, brange);

    bexpected = new ArrayList<byte[]>();
    bexpected.add(bB);
    bexpected.add(bC);

    brange = cuckooJedis.lrange(bfoo, 1, 2);
    assertByteArrayListEquals(bexpected, brange);

    assertNull(cuckooJedis.lrange(bfoo, 2, 1));

  }

  @Test
  public void ltrim() {
    cuckooJedis.lpush("foo", "1");
    cuckooJedis.lpush("foo", "2");
    cuckooJedis.lpush("foo", "3");
    String status = cuckooJedis.ltrim("foo", 0, 1);

    List<String> expected = new ArrayList<String>();
    expected.add("3");
    expected.add("2");

    assertEquals("OK", status);
    assertEquals(2, cuckooJedis.llen("foo").intValue());
    assertEquals(expected, cuckooJedis.lrange("foo", 0, 100));

    // Binary
    cuckooJedis.lpush(bfoo, b1);
    cuckooJedis.lpush(bfoo, b2);
    cuckooJedis.lpush(bfoo, b3);
    String bstatus = cuckooJedis.ltrim(bfoo, 0, 1);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b3);
    bexpected.add(b2);

    assertEquals("OK", bstatus);
    assertEquals(2, cuckooJedis.llen(bfoo).intValue());
    assertByteArrayListEquals(bexpected, cuckooJedis.lrange(bfoo, 0, 100));

  }

  @Test
  public void lset() {
    cuckooJedis.lpush("foo", "1");
    cuckooJedis.lpush("foo", "2");
    cuckooJedis.lpush("foo", "3");

    List<String> expected = new ArrayList<String>();
    expected.add("3");
    expected.add("bar");
    expected.add("1");

    String status = cuckooJedis.lset("foo", 1, "bar");

    assertEquals("OK", status);
    assertEquals(expected, cuckooJedis.lrange("foo", 0, 100));

    // Binary
    cuckooJedis.lpush(bfoo, b1);
    cuckooJedis.lpush(bfoo, b2);
    cuckooJedis.lpush(bfoo, b3);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b3);
    bexpected.add(bbar);
    bexpected.add(b1);

    String bstatus = cuckooJedis.lset(bfoo, 1, bbar);

    assertEquals("OK", bstatus);
    assertByteArrayListEquals(bexpected, cuckooJedis.lrange(bfoo, 0, 100));
  }

  @Test
  public void lindex() {
    cuckooJedis.lpush("foo", "1");
    cuckooJedis.lpush("foo", "2");
    cuckooJedis.lpush("foo", "3");

    assertEquals("3", cuckooJedis.lindex("foo", 0));
    assertNull(cuckooJedis.lindex("foo", 100));

    // Binary
    cuckooJedis.lpush(bfoo, b1);
    cuckooJedis.lpush(bfoo, b2);
    cuckooJedis.lpush(bfoo, b3);

    assertArrayEquals(b3, cuckooJedis.lindex(bfoo, 0));
    assertNull(cuckooJedis.lindex(bfoo, 100));

  }

  @Test
  public void lrem() {
    cuckooJedis.lpush("foo", "hello");
    cuckooJedis.lpush("foo", "hello");
    cuckooJedis.lpush("foo", "x");
    cuckooJedis.lpush("foo", "hello");
    cuckooJedis.lpush("foo", "c");
    cuckooJedis.lpush("foo", "b");
    cuckooJedis.lpush("foo", "a");

    long count = cuckooJedis.lrem("foo", -2, "hello");

    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");
    expected.add("hello");
    expected.add("x");

    assertEquals(2, count);
    assertEquals(expected, cuckooJedis.lrange("foo", 0, 1000));
    assertEquals(0, cuckooJedis.lrem("bar", 100, "foo").intValue());

    // Binary
    cuckooJedis.lpush(bfoo, bhello);
    cuckooJedis.lpush(bfoo, bhello);
    cuckooJedis.lpush(bfoo, bx);
    cuckooJedis.lpush(bfoo, bhello);
    cuckooJedis.lpush(bfoo, bC);
    cuckooJedis.lpush(bfoo, bB);
    cuckooJedis.lpush(bfoo, bA);

    long bcount = cuckooJedis.lrem(bfoo, -2, bhello);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);
    bexpected.add(bC);
    bexpected.add(bhello);
    bexpected.add(bx);

    assertEquals(2, bcount);
    assertByteArrayListEquals(bexpected, cuckooJedis.lrange(bfoo, 0, 1000));
    assertEquals(0, cuckooJedis.lrem(bbar, 100, bfoo).intValue());

  }

  @Test
  public void lpop() {
    cuckooJedis.rpush("foo", "a");
    cuckooJedis.rpush("foo", "b");
    cuckooJedis.rpush("foo", "c");

    String element = cuckooJedis.lpop("foo");
    assertEquals("a", element);

    List<String> expected = new ArrayList<String>();
    expected.add("b");
    expected.add("c");

    assertEquals(expected, cuckooJedis.lrange("foo", 0, 1000));
    cuckooJedis.lpop("foo");
    cuckooJedis.lpop("foo");

    element = cuckooJedis.lpop("foo");
    assertNull(element);

    // Binary
    cuckooJedis.rpush(bfoo, bA);
    cuckooJedis.rpush(bfoo, bB);
    cuckooJedis.rpush(bfoo, bC);

    byte[] belement = cuckooJedis.lpop(bfoo);
    assertArrayEquals(bA, belement);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bB);
    bexpected.add(bC);

    assertByteArrayListEquals(bexpected, cuckooJedis.lrange(bfoo, 0, 1000));
    cuckooJedis.lpop(bfoo);
    cuckooJedis.lpop(bfoo);

    belement = cuckooJedis.lpop(bfoo);
    assertNull(belement);

  }

  @Test
  public void rpop() {
    cuckooJedis.rpush("foo", "a");
    cuckooJedis.rpush("foo", "b");
    cuckooJedis.rpush("foo", "c");

    String element = cuckooJedis.rpop("foo");
    assertEquals("c", element);

    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");

    assertEquals(expected, cuckooJedis.lrange("foo", 0, 1000));
    cuckooJedis.rpop("foo");
    cuckooJedis.rpop("foo");

    element = cuckooJedis.rpop("foo");
    assertNull(element);

    // Binary
    cuckooJedis.rpush(bfoo, bA);
    cuckooJedis.rpush(bfoo, bB);
    cuckooJedis.rpush(bfoo, bC);

    byte[] belement = cuckooJedis.rpop(bfoo);
    assertArrayEquals(bC, belement);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);

    assertByteArrayListEquals(bexpected, cuckooJedis.lrange(bfoo, 0, 1000));
    cuckooJedis.rpop(bfoo);
    cuckooJedis.rpop(bfoo);

    belement = cuckooJedis.rpop(bfoo);
    assertNull(belement);

  }

  @Test
  public void rpoplpush() {
    cuckooJedis.rpush("foo", "a");
    cuckooJedis.rpush("foo", "b");
    cuckooJedis.rpush("foo", "c");

    cuckooJedis.rpush("dst", "foo");
    cuckooJedis.rpush("dst", "bar");

    String element = cuckooJedis.rpoplpush("foo", "dst");

    assertEquals("c", element);

    List<String> srcExpected = new ArrayList<String>();
    srcExpected.add("a");
    srcExpected.add("b");

    List<String> dstExpected = new ArrayList<String>();
    dstExpected.add("c");
    dstExpected.add("foo");
    dstExpected.add("bar");

    assertEquals(srcExpected, cuckooJedis.lrange("foo", 0, 1000));
    assertEquals(dstExpected, cuckooJedis.lrange("dst", 0, 1000));

    // Binary
    cuckooJedis.rpush(bfoo, bA);
    cuckooJedis.rpush(bfoo, bB);
    cuckooJedis.rpush(bfoo, bC);

    cuckooJedis.rpush(bdst, bfoo);
    cuckooJedis.rpush(bdst, bbar);

    byte[] belement = cuckooJedis.rpoplpush(bfoo, bdst);

    assertArrayEquals(bC, belement);

    List<byte[]> bsrcExpected = new ArrayList<byte[]>();
    bsrcExpected.add(bA);
    bsrcExpected.add(bB);

    List<byte[]> bdstExpected = new ArrayList<byte[]>();
    bdstExpected.add(bC);
    bdstExpected.add(bfoo);
    bdstExpected.add(bbar);

    assertByteArrayListEquals(bsrcExpected, cuckooJedis.lrange(bfoo, 0, 1000));
    assertByteArrayListEquals(bdstExpected, cuckooJedis.lrange(bdst, 0, 1000));

  }

  @Test
  public void blpop() throws InterruptedException {
    List<String> result = cuckooJedis.blpop(1, "foo");
    assertNull(result);

    cuckooJedis.lpush("foo", "bar");
    result = cuckooJedis.blpop(1, "foo");

    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("foo", result.get(0));
    assertEquals("bar", result.get(1));

    // Binary
    cuckooJedis.lpush(bfoo, bbar);
    List<byte[]> bresult = cuckooJedis.blpop(1, bfoo);

    assertNotNull(bresult);
    assertEquals(2, bresult.size());
    assertArrayEquals(bfoo, bresult.get(0));
    assertArrayEquals(bbar, bresult.get(1));

  }

  @Test
  public void brpop() throws InterruptedException {
    List<String> result = cuckooJedis.brpop(1, "foo");
    assertNull(result);

    cuckooJedis.lpush("foo", "bar");
    result = cuckooJedis.brpop(1, "foo");
    assertNotNull(result);
    assertEquals(2, result.size());
    assertEquals("foo", result.get(0));
    assertEquals("bar", result.get(1));

    // Binary

    cuckooJedis.lpush(bfoo, bbar);
    List<byte[]> bresult = cuckooJedis.brpop(1, bfoo);
    assertNotNull(bresult);
    assertEquals(2, bresult.size());
    assertArrayEquals(bfoo, bresult.get(0));
    assertArrayEquals(bbar, bresult.get(1));

  }

  @Test
  public void lpushx() {
    long status = cuckooJedis.lpushx("foo", "bar");
    assertEquals(0, status);

    cuckooJedis.lpush("foo", "a");
    status = cuckooJedis.lpushx("foo", "b");
    assertEquals(2, status);

    // Binary
    long bstatus = cuckooJedis.lpushx(bfoo, bbar);
    assertEquals(0, bstatus);

    cuckooJedis.lpush(bfoo, bA);
    bstatus = cuckooJedis.lpushx(bfoo, bB);
    assertEquals(2, bstatus);

  }

  @Test
  public void rpushx() {
    long status = cuckooJedis.rpushx("foo", "bar");
    assertEquals(0, status);

    cuckooJedis.lpush("foo", "a");
    status = cuckooJedis.rpushx("foo", "b");
    assertEquals(2, status);

    // Binary
    long bstatus = cuckooJedis.rpushx(bfoo, bbar);
    assertEquals(0, bstatus);

    cuckooJedis.lpush(bfoo, bA);
    bstatus = cuckooJedis.rpushx(bfoo, bB);
    assertEquals(2, bstatus);
  }

  @Test
  public void linsert() {
    long status = cuckooJedis.linsert("foo", ListPosition.BEFORE, "bar", "car");
    assertEquals(0, status);

    cuckooJedis.lpush("foo", "a");
    status = cuckooJedis.linsert("foo", ListPosition.AFTER, "a", "b");
    assertEquals(2, status);

    List<String> actual = cuckooJedis.lrange("foo", 0, 100);
    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");

    assertEquals(expected, actual);

    status = cuckooJedis.linsert("foo", ListPosition.BEFORE, "bar", "car");
    assertEquals(-1, status);

    // Binary
    long bstatus = cuckooJedis.linsert(bfoo, ListPosition.BEFORE, bbar, bcar);
    assertEquals(0, bstatus);

    cuckooJedis.lpush(bfoo, bA);
    bstatus = cuckooJedis.linsert(bfoo, ListPosition.AFTER, bA, bB);
    assertEquals(2, bstatus);

    List<byte[]> bactual = cuckooJedis.lrange(bfoo, 0, 100);
    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bA);
    bexpected.add(bB);

    assertByteArrayListEquals(bexpected, bactual);

    bstatus = cuckooJedis.linsert(bfoo, ListPosition.BEFORE, bbar, bcar);
    assertEquals(-1, bstatus);

  }

  @Test
  public void brpoplpush() {
    (new Thread(new Runnable() {
      public void run() {
        try {
          Thread.sleep(100);
          CuckooJedis j = createJedis();
          j.lpush("foo", "a");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    })).start();

    String element = cuckooJedis.brpoplpush("foo", "bar", 0);

    assertEquals("a", element);
    assertEquals(1, cuckooJedis.llen("bar").longValue());
    assertEquals("a", cuckooJedis.lrange("bar", 0, -1).get(0));

    (new Thread(new Runnable() {
      public void run() {
        try {
          Thread.sleep(100);
          CuckooJedis j = createJedis();
          j.lpush("foo", "a");
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    })).start();

    byte[] brpoplpush = cuckooJedis.brpoplpush("foo".getBytes(), "bar".getBytes(), 0);

    assertTrue(Arrays.equals("a".getBytes(), brpoplpush));
    assertEquals(1, cuckooJedis.llen("bar").longValue());
    assertEquals("a", cuckooJedis.lrange("bar", 0, -1).get(0));

  }
}
