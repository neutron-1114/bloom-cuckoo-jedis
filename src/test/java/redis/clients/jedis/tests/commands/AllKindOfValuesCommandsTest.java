package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static redis.clients.jedis.Protocol.Command.GET;
import static redis.clients.jedis.Protocol.Command.LRANGE;
import static redis.clients.jedis.Protocol.Command.PING;
import static redis.clients.jedis.Protocol.Command.RPUSH;
import static redis.clients.jedis.Protocol.Command.SET;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;
import static redis.clients.jedis.params.SetParams.setParams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import redis.clients.jedis.CuckooJedis;

import redis.clients.jedis.Protocol.Keyword;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;
import redis.clients.jedis.exceptions.JedisDataException;

public class AllKindOfValuesCommandsTest extends CuckooJedisCommandTestBase {
  final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
  final byte[] bfoo1 = { 0x01, 0x02, 0x03, 0x04, 0x0A };
  final byte[] bfoo2 = { 0x01, 0x02, 0x03, 0x04, 0x0B };
  final byte[] bfoo3 = { 0x01, 0x02, 0x03, 0x04, 0x0C };
  final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
  final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, 0x0A };
  final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, 0x0B };
  final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, 0x0C };

  final byte[] bfoobar = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };
  final byte[] bfoostar = { 0x01, 0x02, 0x03, 0x04, '*' };
  final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };

  final byte[] bnx = { 0x6E, 0x78 };
  final byte[] bex = { 0x65, 0x78 };
  final int expireSeconds = 2;

  @Test
  public void ping() {
    String status = cuckooJedis.ping();
    assertEquals("PONG", status);
  }

  @Test
  public void pingWithMessage() {
    String argument = "message";
    assertEquals(argument, cuckooJedis.ping(argument));
    
    assertArrayEquals(bfoobar, cuckooJedis.ping(bfoobar));
  }

  @Test
  public void exists() {
    String status = cuckooJedis.set("foo", "bar");
    assertEquals("OK", status);

    status = cuckooJedis.set(bfoo, bbar);
    assertEquals("OK", status);

    boolean reply = cuckooJedis.exists("foo");
    assertTrue(reply);

    reply = cuckooJedis.exists(bfoo);
    assertTrue(reply);

    long lreply = cuckooJedis.del("foo");
    assertEquals(1, lreply);

    lreply = cuckooJedis.del(bfoo);
    assertEquals(1, lreply);

    reply = cuckooJedis.exists("foo");
    assertFalse(reply);

    reply = cuckooJedis.exists(bfoo);
    assertFalse(reply);
  }

  @Test
  public void existsMany() {
    String status = cuckooJedis.set("foo1", "bar1");
    assertEquals("OK", status);

    status = cuckooJedis.set("foo2", "bar2");
    assertEquals("OK", status);

    long reply = cuckooJedis.exists("foo1", "foo2");
    assertEquals(2, reply);

    long lreply = cuckooJedis.del("foo1");
    assertEquals(1, lreply);

    reply = cuckooJedis.exists("foo1", "foo2");
    assertEquals(1, reply);
  }

  @Test
  public void del() {
    cuckooJedis.set("foo1", "bar1");
    cuckooJedis.set("foo2", "bar2");
    cuckooJedis.set("foo3", "bar3");

    long reply = cuckooJedis.del("foo1", "foo2", "foo3");
    assertEquals(3, reply);

    Boolean breply = cuckooJedis.exists("foo1");
    assertFalse(breply);
    breply = cuckooJedis.exists("foo2");
    assertFalse(breply);
    breply = cuckooJedis.exists("foo3");
    assertFalse(breply);

    cuckooJedis.set("foo1", "bar1");

    reply = cuckooJedis.del("foo1", "foo2");
    assertEquals(1, reply);

    reply = cuckooJedis.del("foo1", "foo2");
    assertEquals(0, reply);

    // Binary ...
    cuckooJedis.set(bfoo1, bbar1);
    cuckooJedis.set(bfoo2, bbar2);
    cuckooJedis.set(bfoo3, bbar3);

    reply = cuckooJedis.del(bfoo1, bfoo2, bfoo3);
    assertEquals(3, reply);

    breply = cuckooJedis.exists(bfoo1);
    assertFalse(breply);
    breply = cuckooJedis.exists(bfoo2);
    assertFalse(breply);
    breply = cuckooJedis.exists(bfoo3);
    assertFalse(breply);

    cuckooJedis.set(bfoo1, bbar1);

    reply = cuckooJedis.del(bfoo1, bfoo2);
    assertEquals(1, reply);

    reply = cuckooJedis.del(bfoo1, bfoo2);
    assertEquals(0, reply);
  }

  @Test
  public void unlink() {
    cuckooJedis.set("foo1", "bar1");
    cuckooJedis.set("foo2", "bar2");
    cuckooJedis.set("foo3", "bar3");

    long reply = cuckooJedis.unlink("foo1", "foo2", "foo3");
    assertEquals(3, reply);

    reply = cuckooJedis.exists("foo1", "foo2", "foo3");
    assertEquals(0, reply);

    cuckooJedis.set("foo1", "bar1");

    reply = cuckooJedis.unlink("foo1", "foo2");
    assertEquals(1, reply);

    reply = cuckooJedis.unlink("foo1", "foo2");
    assertEquals(0, reply);

    // Binary ...
    cuckooJedis.set(bfoo1, bbar1);
    cuckooJedis.set(bfoo2, bbar2);
    cuckooJedis.set(bfoo3, bbar3);

    reply = cuckooJedis.unlink(bfoo1, bfoo2, bfoo3);
    assertEquals(3, reply);

    reply = cuckooJedis.exists(bfoo1, bfoo2, bfoo3);
    assertEquals(0, reply);

    cuckooJedis.set(bfoo1, bbar1);

    reply = cuckooJedis.unlink(bfoo1, bfoo2);
    assertEquals(1, reply);

    reply = cuckooJedis.unlink(bfoo1, bfoo2);
    assertEquals(0, reply);
  }

  @Test
  public void type() {
    cuckooJedis.set("foo", "bar");
    String status = cuckooJedis.type("foo");
    assertEquals("string", status);

    // Binary
    cuckooJedis.set(bfoo, bbar);
    status = cuckooJedis.type(bfoo);
    assertEquals("string", status);
  }

  @Test
  public void keys() {
    cuckooJedis.set("foo", "bar");
    cuckooJedis.set("foobar", "bar");

    Set<String> keys = cuckooJedis.keys("foo*");
    Set<String> expected = new HashSet<String>();
    expected.add("foo");
    expected.add("foobar");
    assertEquals(expected, keys);

    expected = new HashSet<String>();
    keys = cuckooJedis.keys("bar*");

    assertEquals(expected, keys);

    // Binary
    cuckooJedis.set(bfoo, bbar);
    cuckooJedis.set(bfoobar, bbar);

    Set<byte[]> bkeys = cuckooJedis.keys(bfoostar);
    assertEquals(2, bkeys.size());
    assertTrue(setContains(bkeys, bfoo));
    assertTrue(setContains(bkeys, bfoobar));

    bkeys = cuckooJedis.keys(bbarstar);

    assertEquals(0, bkeys.size());
  }

  @Test
  public void randomKey() {
    assertNull(cuckooJedis.randomKey());

    cuckooJedis.set("foo", "bar");

    assertEquals("foo", cuckooJedis.randomKey());

    cuckooJedis.set("bar", "foo");

    String randomkey = cuckooJedis.randomKey();
    assertTrue(randomkey.equals("foo") || randomkey.equals("bar"));

    // Binary
    cuckooJedis.del("foo");
    cuckooJedis.del("bar");
    assertNull(cuckooJedis.randomKey());

    cuckooJedis.set(bfoo, bbar);

    assertArrayEquals(bfoo, cuckooJedis.randomBinaryKey());

    cuckooJedis.set(bbar, bfoo);

    byte[] randomBkey = cuckooJedis.randomBinaryKey();
    assertTrue(Arrays.equals(randomBkey, bfoo) || Arrays.equals(randomBkey, bbar));

  }

  @Test
  public void rename() {
    cuckooJedis.set("foo", "bar");
    String status = cuckooJedis.rename("foo", "bar");
    assertEquals("OK", status);

    String value = cuckooJedis.get("foo");
    assertNull(value);

    value = cuckooJedis.get("bar");
    assertEquals("bar", value);

    // Binary
    cuckooJedis.set(bfoo, bbar);
    String bstatus = cuckooJedis.rename(bfoo, bbar);
    assertEquals("OK", bstatus);

    byte[] bvalue = cuckooJedis.get(bfoo);
    assertNull(bvalue);

    bvalue = cuckooJedis.get(bbar);
    assertArrayEquals(bbar, bvalue);
  }

  @Test
  public void renameOldAndNewAreTheSame() {
    cuckooJedis.set("foo", "bar");
    cuckooJedis.rename("foo", "foo");

    // Binary
    cuckooJedis.set(bfoo, bbar);
    cuckooJedis.rename(bfoo, bfoo);
  }

  @Test
  public void renamenx() {
    cuckooJedis.set("foo", "bar");
    long status = cuckooJedis.renamenx("foo", "bar");
    assertEquals(1, status);

    cuckooJedis.set("foo", "bar");
    status = cuckooJedis.renamenx("foo", "bar");
    assertEquals(0, status);

    // Binary
    cuckooJedis.set(bfoo, bbar);
    long bstatus = cuckooJedis.renamenx(bfoo, bbar);
    assertEquals(1, bstatus);

    cuckooJedis.set(bfoo, bbar);
    bstatus = cuckooJedis.renamenx(bfoo, bbar);
    assertEquals(0, bstatus);

  }

  @Test
  public void dbSize() {
    long size = cuckooJedis.dbSize();
    assertEquals(0, size);

    cuckooJedis.set("foo", "bar");
    size = cuckooJedis.dbSize();
    assertEquals(1, size);

    // Binary
    cuckooJedis.set(bfoo, bbar);
    size = cuckooJedis.dbSize();
    assertEquals(2, size);
  }

  @Test
  public void expire() {
    long status = cuckooJedis.expire("foo", 20);
    assertEquals(0, status);

    cuckooJedis.set("foo", "bar");
    status = cuckooJedis.expire("foo", 20);
    assertEquals(1, status);

    // Binary
    long bstatus = cuckooJedis.expire(bfoo, 20);
    assertEquals(0, bstatus);

    cuckooJedis.set(bfoo, bbar);
    bstatus = cuckooJedis.expire(bfoo, 20);
    assertEquals(1, bstatus);

  }

  @Test
  public void expireAt() {
    long unixTime = (System.currentTimeMillis() / 1000L) + 20;

    long status = cuckooJedis.expireAt("foo", unixTime);
    assertEquals(0, status);

    cuckooJedis.set("foo", "bar");
    unixTime = (System.currentTimeMillis() / 1000L) + 20;
    status = cuckooJedis.expireAt("foo", unixTime);
    assertEquals(1, status);

    // Binary
    long bstatus = cuckooJedis.expireAt(bfoo, unixTime);
    assertEquals(0, bstatus);

    cuckooJedis.set(bfoo, bbar);
    unixTime = (System.currentTimeMillis() / 1000L) + 20;
    bstatus = cuckooJedis.expireAt(bfoo, unixTime);
    assertEquals(1, bstatus);

  }

  @Test
  public void ttl() {
    long ttl = cuckooJedis.ttl("foo");
    assertEquals(-2, ttl);

    cuckooJedis.set("foo", "bar");
    ttl = cuckooJedis.ttl("foo");
    assertEquals(-1, ttl);

    cuckooJedis.expire("foo", 20);
    ttl = cuckooJedis.ttl("foo");
    assertTrue(ttl >= 0 && ttl <= 20);

    // Binary
    long bttl = cuckooJedis.ttl(bfoo);
    assertEquals(-2, bttl);

    cuckooJedis.set(bfoo, bbar);
    bttl = cuckooJedis.ttl(bfoo);
    assertEquals(-1, bttl);

    cuckooJedis.expire(bfoo, 20);
    bttl = cuckooJedis.ttl(bfoo);
    assertTrue(bttl >= 0 && bttl <= 20);

  }

  @Test
  public void touch() throws Exception {
    long reply = cuckooJedis.touch("foo1", "foo2", "foo3");
    assertEquals(0, reply);

    cuckooJedis.set("foo1", "bar1");

    Thread.sleep(1100); // little over 1 sec
    assertTrue(cuckooJedis.objectIdletime("foo1") > 0);

    reply = cuckooJedis.touch("foo1");
    assertEquals(1, reply);
    assertTrue(cuckooJedis.objectIdletime("foo1") == 0);

    reply = cuckooJedis.touch("foo1", "foo2", "foo3");
    assertEquals(1, reply);

    cuckooJedis.set("foo2", "bar2");

    cuckooJedis.set("foo3", "bar3");

    reply = cuckooJedis.touch("foo1", "foo2", "foo3");
    assertEquals(3, reply);

    // Binary
    reply = cuckooJedis.touch(bfoo1, bfoo2, bfoo3);
    assertEquals(0, reply);

    cuckooJedis.set(bfoo1, bbar1);

    Thread.sleep(1100); // little over 1 sec
    assertTrue(cuckooJedis.objectIdletime(bfoo1) > 0);

    reply = cuckooJedis.touch(bfoo1);
    assertEquals(1, reply);
    assertTrue(cuckooJedis.objectIdletime(bfoo1) == 0);

    reply = cuckooJedis.touch(bfoo1, bfoo2, bfoo3);
    assertEquals(1, reply);

    cuckooJedis.set(bfoo2, bbar2);

    cuckooJedis.set(bfoo3, bbar3);

    reply = cuckooJedis.touch(bfoo1, bfoo2, bfoo3);
    assertEquals(3, reply);

  }

  @Test
  public void select() {
    cuckooJedis.set("foo", "bar");
    String status = cuckooJedis.select(1);
    assertEquals("OK", status);
    assertNull(cuckooJedis.get("foo"));
    status = cuckooJedis.select(0);
    assertEquals("OK", status);
    assertEquals("bar", cuckooJedis.get("foo"));
    // Binary
    cuckooJedis.set(bfoo, bbar);
    String bstatus = cuckooJedis.select(1);
    assertEquals("OK", bstatus);
    assertNull(cuckooJedis.get(bfoo));
    bstatus = cuckooJedis.select(0);
    assertEquals("OK", bstatus);
    assertArrayEquals(bbar, cuckooJedis.get(bfoo));
  }

  @Test
  public void getDB() {
    assertEquals(0, cuckooJedis.getDB());
    cuckooJedis.select(1);
    assertEquals(1, cuckooJedis.getDB());
  }

  @Test
  public void move() {
    long status = cuckooJedis.move("foo", 1);
    assertEquals(0, status);

    cuckooJedis.set("foo", "bar");
    status = cuckooJedis.move("foo", 1);
    assertEquals(1, status);
    assertNull(cuckooJedis.get("foo"));

    cuckooJedis.select(1);
    assertEquals("bar", cuckooJedis.get("foo"));

    // Binary
    cuckooJedis.select(0);
    long bstatus = cuckooJedis.move(bfoo, 1);
    assertEquals(0, bstatus);

    cuckooJedis.set(bfoo, bbar);
    bstatus = cuckooJedis.move(bfoo, 1);
    assertEquals(1, bstatus);
    assertNull(cuckooJedis.get(bfoo));

    cuckooJedis.select(1);
    assertArrayEquals(bbar, cuckooJedis.get(bfoo));

  }

  @Test
  public void swapDB() {
    cuckooJedis.set("foo1", "bar1");
    cuckooJedis.select(1);
    assertNull(cuckooJedis.get("foo1"));
    cuckooJedis.set("foo2", "bar2");
    String status = cuckooJedis.swapDB(0, 1);
    assertEquals("OK", status);
    assertEquals("bar1", cuckooJedis.get("foo1"));
    assertNull(cuckooJedis.get("foo2"));
    cuckooJedis.select(0);
    assertNull(cuckooJedis.get("foo1"));
    assertEquals("bar2", cuckooJedis.get("foo2"));

    // Binary
    cuckooJedis.set(bfoo1, bbar1);
    cuckooJedis.select(1);
    assertArrayEquals(null, cuckooJedis.get(bfoo1));
    cuckooJedis.set(bfoo2, bbar2);
    status = cuckooJedis.swapDB(0, 1);
    assertEquals("OK", status);
    assertArrayEquals(bbar1, cuckooJedis.get(bfoo1));
    assertArrayEquals(null, cuckooJedis.get(bfoo2));
    cuckooJedis.select(0);
    assertArrayEquals(null, cuckooJedis.get(bfoo1));
    assertArrayEquals(bbar2, cuckooJedis.get(bfoo2));
  }

  @Test
  public void flushDB() {
    cuckooJedis.set("foo", "bar");
    assertEquals(1, cuckooJedis.dbSize().intValue());
    cuckooJedis.set("bar", "foo");
    cuckooJedis.move("bar", 1);
    String status = cuckooJedis.flushDB();
    assertEquals("OK", status);
    assertEquals(0, cuckooJedis.dbSize().intValue());
    cuckooJedis.select(1);
    assertEquals(1, cuckooJedis.dbSize().intValue());
    cuckooJedis.del("bar");

    // Binary
    cuckooJedis.select(0);
    cuckooJedis.set(bfoo, bbar);
    assertEquals(1, cuckooJedis.dbSize().intValue());
    cuckooJedis.set(bbar, bfoo);
    cuckooJedis.move(bbar, 1);
    String bstatus = cuckooJedis.flushDB();
    assertEquals("OK", bstatus);
    assertEquals(0, cuckooJedis.dbSize().intValue());
    cuckooJedis.select(1);
    assertEquals(1, cuckooJedis.dbSize().intValue());

  }

  @Test
  public void flushAll() {
    cuckooJedis.set("foo", "bar");
    assertEquals(1, cuckooJedis.dbSize().intValue());
    cuckooJedis.set("bar", "foo");
    cuckooJedis.move("bar", 1);
    String status = cuckooJedis.flushAll();
    assertEquals("OK", status);
    assertEquals(0, cuckooJedis.dbSize().intValue());
    cuckooJedis.select(1);
    assertEquals(0, cuckooJedis.dbSize().intValue());

    // Binary
    cuckooJedis.select(0);
    cuckooJedis.set(bfoo, bbar);
    assertEquals(1, cuckooJedis.dbSize().intValue());
    cuckooJedis.set(bbar, bfoo);
    cuckooJedis.move(bbar, 1);
    String bstatus = cuckooJedis.flushAll();
    assertEquals("OK", bstatus);
    assertEquals(0, cuckooJedis.dbSize().intValue());
    cuckooJedis.select(1);
    assertEquals(0, cuckooJedis.dbSize().intValue());

  }

  @Test
  public void persist() {
    cuckooJedis.setex("foo", 60 * 60, "bar");
    assertTrue(cuckooJedis.ttl("foo") > 0);
    long status = cuckooJedis.persist("foo");
    assertEquals(1, status);
    assertEquals(-1, cuckooJedis.ttl("foo").intValue());

    // Binary
    cuckooJedis.setex(bfoo, 60 * 60, bbar);
    assertTrue(cuckooJedis.ttl(bfoo) > 0);
    long bstatus = cuckooJedis.persist(bfoo);
    assertEquals(1, bstatus);
    assertEquals(-1, cuckooJedis.ttl(bfoo).intValue());

  }

  @Test
  public void echo() {
    String result = cuckooJedis.echo("hello world");
    assertEquals("hello world", result);

    // Binary
    byte[] bresult = cuckooJedis.echo(SafeEncoder.encode("hello world"));
    assertArrayEquals(SafeEncoder.encode("hello world"), bresult);
  }

  @Test
  public void dumpAndRestore() {
    cuckooJedis.set("foo1", "bar");
    byte[] sv = cuckooJedis.dump("foo1");
    cuckooJedis.restore("foo2", 0, sv);
    assertEquals("bar", cuckooJedis.get("foo2"));
  }

  @Test
  public void restoreReplace() {
    // take a separate instance
    CuckooJedis cuckooJedis2 = new CuckooJedis(hnp.getHost(), 6380, 500);
    cuckooJedis2.auth("foobared");
    cuckooJedis2.flushAll();

    cuckooJedis2.set("foo", "bar");

    Map<String, String> map = new HashMap<String, String>();
    map.put("a", "A");
    map.put("b", "B");

    cuckooJedis.hset("from", map);
    byte[] serialized = cuckooJedis.dump("from");

    try {
      cuckooJedis2.restore("foo", 0, serialized);
      fail("Simple restore on a existing key should fail");
    } catch(JedisDataException e) {
      // should be here
    }
    assertEquals("bar", cuckooJedis2.get("foo"));

    cuckooJedis2.restoreReplace("foo", 0, serialized);
    assertEquals(map, cuckooJedis2.hgetAll("foo"));

    cuckooJedis2.close();
  }

  @Test
  public void pexpire() {
    long status = cuckooJedis.pexpire("foo", 10000);
    assertEquals(0, status);

    cuckooJedis.set("foo1", "bar1");
    status = cuckooJedis.pexpire("foo1", 10000);
    assertEquals(1, status);

    cuckooJedis.set("foo2", "bar2");
    status = cuckooJedis.pexpire("foo2", 200000000000L);
    assertEquals(1, status);

    long pttl = cuckooJedis.pttl("foo2");
    assertTrue(pttl > 100000000000L);
  }

  @Test
  public void pexpireAt() {
    long unixTime = (System.currentTimeMillis()) + 10000;

    long status = cuckooJedis.pexpireAt("foo", unixTime);
    assertEquals(0, status);

    cuckooJedis.set("foo", "bar");
    unixTime = (System.currentTimeMillis()) + 10000;
    status = cuckooJedis.pexpireAt("foo", unixTime);
    assertEquals(1, status);
  }

  @Test
  public void pttl() {
    long pttl = cuckooJedis.pttl("foo");
    assertEquals(-2, pttl);

    cuckooJedis.set("foo", "bar");
    pttl = cuckooJedis.pttl("foo");
    assertEquals(-1, pttl);

    cuckooJedis.pexpire("foo", 20000);
    pttl = cuckooJedis.pttl("foo");
    assertTrue(pttl >= 0 && pttl <= 20000);
  }

  @Test
  public void psetex() {
    long pttl = cuckooJedis.pttl("foo");
    assertEquals(-2, pttl);

    String status = cuckooJedis.psetex("foo", 200000000000L, "bar");
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));

    pttl = cuckooJedis.pttl("foo");
    assertTrue(pttl > 100000000000L);
  }

  @Test
  public void scan() {
    cuckooJedis.set("b", "b");
    cuckooJedis.set("a", "a");

    ScanResult<String> result = cuckooJedis.scan(SCAN_POINTER_START);

    assertEquals(SCAN_POINTER_START, result.getCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    ScanResult<byte[]> bResult = cuckooJedis.scan(SCAN_POINTER_START_BINARY);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void scanMatch() {
    ScanParams params = new ScanParams();
    params.match("a*");

    cuckooJedis.set("b", "b");
    cuckooJedis.set("a", "a");
    cuckooJedis.set("aa", "aa");
    ScanResult<String> result = cuckooJedis.scan(SCAN_POINTER_START, params);

    assertEquals(SCAN_POINTER_START, result.getCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.match(bfoostar);

    cuckooJedis.set(bfoo1, bbar);
    cuckooJedis.set(bfoo2, bbar);
    cuckooJedis.set(bfoo3, bbar);

    ScanResult<byte[]> bResult = cuckooJedis.scan(SCAN_POINTER_START_BINARY, params);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void scanCount() {
    ScanParams params = new ScanParams();
    params.count(2);

    for (int i = 0; i < 10; i++) {
      cuckooJedis.set("a" + i, "a" + i);
    }

    ScanResult<String> result = cuckooJedis.scan(SCAN_POINTER_START, params);

    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.count(2);

    cuckooJedis.set(bfoo1, bbar);
    cuckooJedis.set(bfoo2, bbar);
    cuckooJedis.set(bfoo3, bbar);

    ScanResult<byte[]> bResult = cuckooJedis.scan(SCAN_POINTER_START_BINARY, params);

    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void scanIsCompleteIteration() {
    for (int i = 0; i < 100; i++) {
      cuckooJedis.set("a" + i, "a" + i);
    }

    ScanResult<String> result = cuckooJedis.scan(SCAN_POINTER_START);
    // note: in theory Redis would be allowed to already return all results on the 1st scan,
    // but in practice this never happens for data sets greater than a few tens
    // see: https://redis.io/commands/scan#number-of-elements-returned-at-every-scan-call
    assertFalse(result.isCompleteIteration());

    result = scanCompletely(result.getCursor());

    assertNotNull(result);
    assertTrue(result.isCompleteIteration());
  }

  private ScanResult<String> scanCompletely(String cursor) {
    ScanResult<String> scanResult;
    do {
      scanResult = cuckooJedis.scan(cursor);
      cursor = scanResult.getCursor();
    } while (!SCAN_POINTER_START.equals(scanResult.getCursor()));

    return scanResult;
  }

  @Test
  public void setNxExAndGet() {
    String status = cuckooJedis.set("hello", "world", setParams().nx().ex(expireSeconds));
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    String value = cuckooJedis.get("hello");
    assertEquals("world", value);

    cuckooJedis.set("hello", "bar", setParams().nx().ex(expireSeconds));
    value = cuckooJedis.get("hello");
    assertEquals("world", value);

    long ttl = cuckooJedis.ttl("hello");
    assertTrue(ttl > 0 && ttl <= expireSeconds);

    // binary
    byte[] bworld = { 0x77, 0x6F, 0x72, 0x6C, 0x64 };
    byte[] bhello = { 0x68, 0x65, 0x6C, 0x6C, 0x6F };

    String bstatus = cuckooJedis.set(bworld, bhello, setParams().nx().ex(expireSeconds));
    assertTrue(Keyword.OK.name().equalsIgnoreCase(bstatus));
    byte[] bvalue = cuckooJedis.get(bworld);
    assertTrue(Arrays.equals(bhello, bvalue));

    cuckooJedis.set(bworld, bbar, setParams().nx().ex(expireSeconds));
    bvalue = cuckooJedis.get(bworld);
    assertTrue(Arrays.equals(bhello, bvalue));

    long bttl = cuckooJedis.ttl(bworld);
    assertTrue(bttl > 0 && bttl <= expireSeconds);
  }

  @Test
  public void sendCommandTest(){
    Object obj = cuckooJedis.sendCommand(SET, "x", "1");
    String returnValue = SafeEncoder.encode((byte[]) obj);
    assertEquals("OK", returnValue);
    obj = cuckooJedis.sendCommand(GET, "x");
    returnValue = SafeEncoder.encode((byte[]) obj);
    assertEquals("1", returnValue);

    cuckooJedis.sendCommand(RPUSH,"foo", "a");
    cuckooJedis.sendCommand(RPUSH,"foo", "b");
    cuckooJedis.sendCommand(RPUSH,"foo", "c");

    obj = cuckooJedis.sendCommand(LRANGE,"foo", "0", "2");
    List<byte[]> list = (List<byte[]>) obj;
    List<byte[]> expected = new ArrayList<>(3);
    expected.add("a".getBytes());
    expected.add("b".getBytes());
    expected.add("c".getBytes());
    for (int i = 0; i < 3; i++)
      assertArrayEquals(expected.get(i), list.get(i));

    assertEquals("PONG", SafeEncoder.encode((byte[]) cuckooJedis.sendCommand(PING)));
  }

}
