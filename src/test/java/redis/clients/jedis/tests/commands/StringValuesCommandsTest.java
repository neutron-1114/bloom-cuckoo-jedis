package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import redis.clients.jedis.exceptions.JedisDataException;

public class StringValuesCommandsTest extends CuckooJedisCommandTestBase {
  @Test
  public void setAndGet() {
    String status = cuckooJedis.set("foo", "bar");
    assertEquals("OK", status);

    String value = cuckooJedis.get("foo");
    assertEquals("bar", value);

    assertNull(cuckooJedis.get("bar"));
  }

  @Test
  public void getSet() {
    String value = cuckooJedis.getSet("foo", "bar");
    assertNull(value);
    value = cuckooJedis.get("foo");
    assertEquals("bar", value);
  }

  @Test
  public void mget() {
    List<String> values = cuckooJedis.mget("foo", "bar");
    List<String> expected = new ArrayList<String>();
    expected.add(null);
    expected.add(null);

    assertEquals(expected, values);

    cuckooJedis.set("foo", "bar");

    expected = new ArrayList<String>();
    expected.add("bar");
    expected.add(null);
    values = cuckooJedis.mget("foo", "bar");

    assertEquals(expected, values);

    cuckooJedis.set("bar", "foo");

    expected = new ArrayList<String>();
    expected.add("bar");
    expected.add("foo");
    values = cuckooJedis.mget("foo", "bar");

    assertEquals(expected, values);
  }

  @Test
  public void setnx() {
    long status = cuckooJedis.setnx("foo", "bar");
    assertEquals(1, status);
    assertEquals("bar", cuckooJedis.get("foo"));

    status = cuckooJedis.setnx("foo", "bar2");
    assertEquals(0, status);
    assertEquals("bar", cuckooJedis.get("foo"));
  }

  @Test
  public void setex() {
    String status = cuckooJedis.setex("foo", 20, "bar");
    assertEquals("OK", status);
    long ttl = cuckooJedis.ttl("foo");
    assertTrue(ttl > 0 && ttl <= 20);
  }

  @Test
  public void mset() {
    String status = cuckooJedis.mset("foo", "bar", "bar", "foo");
    assertEquals("OK", status);
    assertEquals("bar", cuckooJedis.get("foo"));
    assertEquals("foo", cuckooJedis.get("bar"));
  }

  @Test
  public void msetnx() {
    long status = cuckooJedis.msetnx("foo", "bar", "bar", "foo");
    assertEquals(1, status);
    assertEquals("bar", cuckooJedis.get("foo"));
    assertEquals("foo", cuckooJedis.get("bar"));

    status = cuckooJedis.msetnx("foo", "bar1", "bar2", "foo2");
    assertEquals(0, status);
    assertEquals("bar", cuckooJedis.get("foo"));
    assertEquals("foo", cuckooJedis.get("bar"));
  }

  @Test(expected = JedisDataException.class)
  public void incrWrongValue() {
    cuckooJedis.set("foo", "bar");
    cuckooJedis.incr("foo");
  }

  @Test
  public void incr() {
    long value = cuckooJedis.incr("foo");
    assertEquals(1, value);
    value = cuckooJedis.incr("foo");
    assertEquals(2, value);
  }

  @Test(expected = JedisDataException.class)
  public void incrByWrongValue() {
    cuckooJedis.set("foo", "bar");
    cuckooJedis.incrBy("foo", 2);
  }

  @Test
  public void incrBy() {
    long value = cuckooJedis.incrBy("foo", 2);
    assertEquals(2, value);
    value = cuckooJedis.incrBy("foo", 2);
    assertEquals(4, value);
  }

  @Test(expected = JedisDataException.class)
  public void incrByFloatWrongValue() {
    cuckooJedis.set("foo", "bar");
    cuckooJedis.incrByFloat("foo", 2d);
  }

  @Test(expected = JedisDataException.class)
  public void decrWrongValue() {
    cuckooJedis.set("foo", "bar");
    cuckooJedis.decr("foo");
  }

  @Test
  public void decr() {
    long value = cuckooJedis.decr("foo");
    assertEquals(-1, value);
    value = cuckooJedis.decr("foo");
    assertEquals(-2, value);
  }

  @Test(expected = JedisDataException.class)
  public void decrByWrongValue() {
    cuckooJedis.set("foo", "bar");
    cuckooJedis.decrBy("foo", 2);
  }

  @Test
  public void decrBy() {
    long value = cuckooJedis.decrBy("foo", 2);
    assertEquals(-2, value);
    value = cuckooJedis.decrBy("foo", 2);
    assertEquals(-4, value);
  }

  @Test
  public void append() {
    long value = cuckooJedis.append("foo", "bar");
    assertEquals(3, value);
    assertEquals("bar", cuckooJedis.get("foo"));
    value = cuckooJedis.append("foo", "bar");
    assertEquals(6, value);
    assertEquals("barbar", cuckooJedis.get("foo"));
  }

  @Test
  public void substr() {
    cuckooJedis.set("s", "This is a string");
    assertEquals("This", cuckooJedis.substr("s", 0, 3));
    assertEquals("ing", cuckooJedis.substr("s", -3, -1));
    assertEquals("This is a string", cuckooJedis.substr("s", 0, -1));
    assertEquals(" string", cuckooJedis.substr("s", 9, 100000));
  }

  @Test
  public void strlen() {
    cuckooJedis.set("s", "This is a string");
    assertEquals("This is a string".length(), cuckooJedis.strlen("s").intValue());
  }

  @Test
  public void incrLargeNumbers() {
    long value = cuckooJedis.incr("foo");
    assertEquals(1, value);
    assertEquals(1L + Integer.MAX_VALUE, (long) cuckooJedis.incrBy("foo", Integer.MAX_VALUE));
  }

  @Test(expected = JedisDataException.class)
  public void incrReallyLargeNumbers() {
    cuckooJedis.set("foo", Long.toString(Long.MAX_VALUE));
    long value = cuckooJedis.incr("foo");
    assertEquals(Long.MIN_VALUE, value);
  }

  @Test
  public void incrByFloat() {
    double value = cuckooJedis.incrByFloat("foo", 10.5);
    assertEquals(10.5, value, 0.0);
    value = cuckooJedis.incrByFloat("foo", 0.1);
    assertEquals(10.6, value, 0.0);
  }

  @Test
  public void psetex() {
    String status = cuckooJedis.psetex("foo", 20000, "bar");
    assertEquals("OK", status);
    long ttl = cuckooJedis.ttl("foo");
    assertTrue(ttl > 0 && ttl <= 20000);
  }
}