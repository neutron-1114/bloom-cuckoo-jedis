package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static redis.clients.jedis.Protocol.Command.GET;
import static redis.clients.jedis.Protocol.Command.LRANGE;
import static redis.clients.jedis.Protocol.Command.RPUSH;
import static redis.clients.jedis.Protocol.Command.SET;
import static redis.clients.jedis.params.SetParams.setParams;
import static redis.clients.jedis.tests.utils.AssertUtil.assertByteArrayListEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Protocol.Keyword;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.SafeEncoder;

public class BinaryValuesCommandsTest extends CuckooJedisCommandTestBase {
  byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
  byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
  byte[] bxx = { 0x78, 0x78 };
  byte[] bnx = { 0x6E, 0x78 };
  byte[] bex = { 0x65, 0x78 };
  byte[] bpx = { 0x70, 0x78 };
  int expireSeconds = 2;
  long expireMillis = expireSeconds * 1000;
  byte[] binaryValue;

  @Before
  public void startUp() {
    StringBuilder sb = new StringBuilder();

    for (int n = 0; n < 1000; n++) {
      sb.append("A");
    }

    binaryValue = sb.toString().getBytes();
  }

  @Test
  public void setAndGet() {
    String status = cuckooJedis.set(bfoo, binaryValue);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));

    byte[] value = cuckooJedis.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));

    assertNull(cuckooJedis.get(bbar));
  }

  @Test
  public void setNxExAndGet() {
    String status = cuckooJedis.set(bfoo, binaryValue, setParams().nx().ex(expireSeconds));
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    byte[] value = cuckooJedis.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));

    assertNull(cuckooJedis.get(bbar));
  }

  @Test
  public void setIfNotExistAndGet() {
    String status = cuckooJedis.set(bfoo, binaryValue);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    // nx should fail if value exists
    String statusFail = cuckooJedis.set(bfoo, binaryValue, setParams().nx().ex(expireSeconds));
    assertNull(statusFail);

    byte[] value = cuckooJedis.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));

    assertNull(cuckooJedis.get(bbar));
  }

  @Test
  public void setIfExistAndGet() {
    String status = cuckooJedis.set(bfoo, binaryValue);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    // nx should fail if value exists
    String statusSuccess = cuckooJedis.set(bfoo, binaryValue, setParams().xx().ex(expireSeconds));
    assertTrue(Keyword.OK.name().equalsIgnoreCase(statusSuccess));

    byte[] value = cuckooJedis.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));

    assertNull(cuckooJedis.get(bbar));
  }

  @Test
  public void setFailIfNotExistAndGet() {
    // xx should fail if value does NOT exists
    String statusFail = cuckooJedis.set(bfoo, binaryValue, setParams().xx().ex(expireSeconds));
    assertNull(statusFail);
  }

  @Test
  public void setAndExpireMillis() {
    String status = cuckooJedis.set(bfoo, binaryValue, setParams().nx().px(expireMillis));
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    long ttl = cuckooJedis.ttl(bfoo);
    assertTrue(ttl > 0 && ttl <= expireSeconds);
  }

  @Test
  public void setAndExpire() {
    String status = cuckooJedis.set(bfoo, binaryValue, setParams().nx().ex(expireSeconds));
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    long ttl = cuckooJedis.ttl(bfoo);
    assertTrue(ttl > 0 && ttl <= expireSeconds);
  }

  @Test
  public void getSet() {
    byte[] value = cuckooJedis.getSet(bfoo, binaryValue);
    assertNull(value);
    value = cuckooJedis.get(bfoo);
    assertTrue(Arrays.equals(binaryValue, value));
  }

  @Test
  public void mget() {
    List<byte[]> values = cuckooJedis.mget(bfoo, bbar);
    List<byte[]> expected = new ArrayList<byte[]>();
    expected.add(null);
    expected.add(null);

    assertByteArrayListEquals(expected, values);

    cuckooJedis.set(bfoo, binaryValue);

    expected = new ArrayList<byte[]>();
    expected.add(binaryValue);
    expected.add(null);
    values = cuckooJedis.mget(bfoo, bbar);

    assertByteArrayListEquals(expected, values);

    cuckooJedis.set(bbar, bfoo);

    expected = new ArrayList<byte[]>();
    expected.add(binaryValue);
    expected.add(bfoo);
    values = cuckooJedis.mget(bfoo, bbar);

    assertByteArrayListEquals(expected, values);
  }

  @Test
  public void setnx() {
    long status = cuckooJedis.setnx(bfoo, binaryValue);
    assertEquals(1, status);
    assertTrue(Arrays.equals(binaryValue, cuckooJedis.get(bfoo)));

    status = cuckooJedis.setnx(bfoo, bbar);
    assertEquals(0, status);
    assertTrue(Arrays.equals(binaryValue, cuckooJedis.get(bfoo)));
  }

  @Test
  public void setex() {
    String status = cuckooJedis.setex(bfoo, 20, binaryValue);
    assertEquals(Keyword.OK.name(), status);
    long ttl = cuckooJedis.ttl(bfoo);
    assertTrue(ttl > 0 && ttl <= 20);
  }

  @Test
  public void mset() {
    String status = cuckooJedis.mset(bfoo, binaryValue, bbar, bfoo);
    assertEquals(Keyword.OK.name(), status);
    assertTrue(Arrays.equals(binaryValue, cuckooJedis.get(bfoo)));
    assertTrue(Arrays.equals(bfoo, cuckooJedis.get(bbar)));
  }

  @Test
  public void msetnx() {
    long status = cuckooJedis.msetnx(bfoo, binaryValue, bbar, bfoo);
    assertEquals(1, status);
    assertTrue(Arrays.equals(binaryValue, cuckooJedis.get(bfoo)));
    assertTrue(Arrays.equals(bfoo, cuckooJedis.get(bbar)));

    status = cuckooJedis.msetnx(bfoo, bbar, "bar2".getBytes(), "foo2".getBytes());
    assertEquals(0, status);
    assertTrue(Arrays.equals(binaryValue, cuckooJedis.get(bfoo)));
    assertTrue(Arrays.equals(bfoo, cuckooJedis.get(bbar)));
  }

  @Test(expected = JedisDataException.class)
  public void incrWrongValue() {
    cuckooJedis.set(bfoo, binaryValue);
    cuckooJedis.incr(bfoo);
  }

  @Test
  public void incr() {
    long value = cuckooJedis.incr(bfoo);
    assertEquals(1, value);
    value = cuckooJedis.incr(bfoo);
    assertEquals(2, value);
  }

  @Test(expected = JedisDataException.class)
  public void incrByWrongValue() {
    cuckooJedis.set(bfoo, binaryValue);
    cuckooJedis.incrBy(bfoo, 2);
  }

  @Test
  public void incrBy() {
    long value = cuckooJedis.incrBy(bfoo, 2);
    assertEquals(2, value);
    value = cuckooJedis.incrBy(bfoo, 2);
    assertEquals(4, value);
  }

  @Test(expected = JedisDataException.class)
  public void decrWrongValue() {
    cuckooJedis.set(bfoo, binaryValue);
    cuckooJedis.decr(bfoo);
  }

  @Test
  public void decr() {
    long value = cuckooJedis.decr(bfoo);
    assertEquals(-1, value);
    value = cuckooJedis.decr(bfoo);
    assertEquals(-2, value);
  }

  @Test(expected = JedisDataException.class)
  public void decrByWrongValue() {
    cuckooJedis.set(bfoo, binaryValue);
    cuckooJedis.decrBy(bfoo, 2);
  }

  @Test
  public void decrBy() {
    long value = cuckooJedis.decrBy(bfoo, 2);
    assertEquals(-2, value);
    value = cuckooJedis.decrBy(bfoo, 2);
    assertEquals(-4, value);
  }

  @Test
  public void append() {
    byte[] first512 = new byte[512];
    System.arraycopy(binaryValue, 0, first512, 0, 512);
    long value = cuckooJedis.append(bfoo, first512);
    assertEquals(512, value);
    assertTrue(Arrays.equals(first512, cuckooJedis.get(bfoo)));

    byte[] rest = new byte[binaryValue.length - 512];
    System.arraycopy(binaryValue, 512, rest, 0, binaryValue.length - 512);
    value = cuckooJedis.append(bfoo, rest);
    assertEquals(binaryValue.length, value);

    assertTrue(Arrays.equals(binaryValue, cuckooJedis.get(bfoo)));
  }

  @Test
  public void substr() {
    cuckooJedis.set(bfoo, binaryValue);

    byte[] first512 = new byte[512];
    System.arraycopy(binaryValue, 0, first512, 0, 512);
    byte[] rfirst512 = cuckooJedis.substr(bfoo, 0, 511);
    assertTrue(Arrays.equals(first512, rfirst512));

    byte[] last512 = new byte[512];
    System.arraycopy(binaryValue, binaryValue.length - 512, last512, 0, 512);
    assertTrue(Arrays.equals(last512, cuckooJedis.substr(bfoo, -512, -1)));

    assertTrue(Arrays.equals(binaryValue, cuckooJedis.substr(bfoo, 0, -1)));

    assertTrue(Arrays.equals(last512, cuckooJedis.substr(bfoo, binaryValue.length - 512, 100000)));
  }

  @Test
  public void strlen() {
    cuckooJedis.set(bfoo, binaryValue);
    assertEquals(binaryValue.length, cuckooJedis.strlen(bfoo).intValue());
  }

  @Test
  public void sendCommandTest(){
    Object obj = cuckooJedis.sendCommand(SET, "x".getBytes(), "1".getBytes());
    String returnValue = SafeEncoder.encode((byte[]) obj);
    assertEquals("OK", returnValue);
    obj = cuckooJedis.sendCommand(GET, "x".getBytes());
    returnValue = SafeEncoder.encode((byte[]) obj);
    assertEquals("1", returnValue);

    cuckooJedis.sendCommand(RPUSH,"foo".getBytes(), "a".getBytes());
    cuckooJedis.sendCommand(RPUSH,"foo".getBytes(), "b".getBytes());
    cuckooJedis.sendCommand(RPUSH,"foo".getBytes(), "c".getBytes());

    obj = cuckooJedis.sendCommand(LRANGE,"foo".getBytes(), "0".getBytes(), "2".getBytes());
    List<byte[]> list = (List<byte[]>) obj;
    List<byte[]> expected = new ArrayList<>(3);
    expected.add("a".getBytes());
    expected.add("b".getBytes());
    expected.add("c".getBytes());
    for (int i = 0; i < 3; i++)
      assertArrayEquals(expected.get(i), list.get(i));
  }
}