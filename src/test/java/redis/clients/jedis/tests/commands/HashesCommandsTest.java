package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;
import static redis.clients.jedis.tests.utils.AssertUtil.assertByteArrayListEquals;
import static redis.clients.jedis.tests.utils.AssertUtil.assertByteArraySetEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class HashesCommandsTest extends CuckooJedisCommandTestBase {
  final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
  final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
  final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };

  final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, 0x0A };
  final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, 0x0B };
  final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, 0x0C };
  final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };

  @Test
  public void hset() {
    long status = cuckooJedis.hset("foo", "bar", "car");
    assertEquals(1, status);
    status = cuckooJedis.hset("foo", "bar", "foo");
    assertEquals(0, status);

    // Binary
    long bstatus = cuckooJedis.hset(bfoo, bbar, bcar);
    assertEquals(1, bstatus);
    bstatus = cuckooJedis.hset(bfoo, bbar, bfoo);
    assertEquals(0, bstatus);

  }

  @Test
  public void hget() {
    cuckooJedis.hset("foo", "bar", "car");
    assertNull(cuckooJedis.hget("bar", "foo"));
    assertNull(cuckooJedis.hget("foo", "car"));
    assertEquals("car", cuckooJedis.hget("foo", "bar"));

    // Binary
    cuckooJedis.hset(bfoo, bbar, bcar);
    assertNull(cuckooJedis.hget(bbar, bfoo));
    assertNull(cuckooJedis.hget(bfoo, bcar));
    assertArrayEquals(bcar, cuckooJedis.hget(bfoo, bbar));
  }

  @Test
  public void hsetnx() {
    long status = cuckooJedis.hsetnx("foo", "bar", "car");
    assertEquals(1, status);
    assertEquals("car", cuckooJedis.hget("foo", "bar"));

    status = cuckooJedis.hsetnx("foo", "bar", "foo");
    assertEquals(0, status);
    assertEquals("car", cuckooJedis.hget("foo", "bar"));

    status = cuckooJedis.hsetnx("foo", "car", "bar");
    assertEquals(1, status);
    assertEquals("bar", cuckooJedis.hget("foo", "car"));

    // Binary
    long bstatus = cuckooJedis.hsetnx(bfoo, bbar, bcar);
    assertEquals(1, bstatus);
    assertArrayEquals(bcar, cuckooJedis.hget(bfoo, bbar));

    bstatus = cuckooJedis.hsetnx(bfoo, bbar, bfoo);
    assertEquals(0, bstatus);
    assertArrayEquals(bcar, cuckooJedis.hget(bfoo, bbar));

    bstatus = cuckooJedis.hsetnx(bfoo, bcar, bbar);
    assertEquals(1, bstatus);
    assertArrayEquals(bbar, cuckooJedis.hget(bfoo, bcar));

  }

  @Test
  public void hmset() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    String status = cuckooJedis.hmset("foo", hash);
    assertEquals("OK", status);
    assertEquals("car", cuckooJedis.hget("foo", "bar"));
    assertEquals("bar", cuckooJedis.hget("foo", "car"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    String bstatus = cuckooJedis.hmset(bfoo, bhash);
    assertEquals("OK", bstatus);
    assertArrayEquals(bcar, cuckooJedis.hget(bfoo, bbar));
    assertArrayEquals(bbar, cuckooJedis.hget(bfoo, bcar));

  }

  @Test
  public void hsetVariadic() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    long status = cuckooJedis.hset("foo", hash);
    assertEquals(2, status);
    assertEquals("car", cuckooJedis.hget("foo", "bar"));
    assertEquals("bar", cuckooJedis.hget("foo", "car"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    status = cuckooJedis.hset(bfoo, bhash);
    assertEquals(2, status);
    assertArrayEquals(bcar, cuckooJedis.hget(bfoo, bbar));
    assertArrayEquals(bbar, cuckooJedis.hget(bfoo, bcar));
  }

  @Test
  public void hmget() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    cuckooJedis.hmset("foo", hash);

    List<String> values = cuckooJedis.hmget("foo", "bar", "car", "foo");
    List<String> expected = new ArrayList<String>();
    expected.add("car");
    expected.add("bar");
    expected.add(null);

    assertEquals(expected, values);

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    cuckooJedis.hmset(bfoo, bhash);

    List<byte[]> bvalues = cuckooJedis.hmget(bfoo, bbar, bcar, bfoo);
    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bcar);
    bexpected.add(bbar);
    bexpected.add(null);

    assertByteArrayListEquals(bexpected, bvalues);
  }

  @Test
  public void hincrBy() {
    long value = cuckooJedis.hincrBy("foo", "bar", 1);
    assertEquals(1, value);
    value = cuckooJedis.hincrBy("foo", "bar", -1);
    assertEquals(0, value);
    value = cuckooJedis.hincrBy("foo", "bar", -10);
    assertEquals(-10, value);

    // Binary
    long bvalue = cuckooJedis.hincrBy(bfoo, bbar, 1);
    assertEquals(1, bvalue);
    bvalue = cuckooJedis.hincrBy(bfoo, bbar, -1);
    assertEquals(0, bvalue);
    bvalue = cuckooJedis.hincrBy(bfoo, bbar, -10);
    assertEquals(-10, bvalue);

  }

  @Test
  public void hincrByFloat() {
    Double value = cuckooJedis.hincrByFloat("foo", "bar", 1.5d);
    assertEquals((Double) 1.5d, value);
    value = cuckooJedis.hincrByFloat("foo", "bar", -1.5d);
    assertEquals((Double) 0d, value);
    value = cuckooJedis.hincrByFloat("foo", "bar", -10.7d);
    assertEquals(Double.valueOf(-10.7d), value);

    // Binary
    double bvalue = cuckooJedis.hincrByFloat(bfoo, bbar, 1.5d);
    assertEquals(1.5d, bvalue, 0d);
    bvalue = cuckooJedis.hincrByFloat(bfoo, bbar, -1.5d);
    assertEquals(0d, bvalue, 0d);
    bvalue = cuckooJedis.hincrByFloat(bfoo, bbar, -10.7d);
    assertEquals(-10.7d, bvalue, 0d);

  }

  @Test
  public void hexists() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    cuckooJedis.hmset("foo", hash);

    assertFalse(cuckooJedis.hexists("bar", "foo"));
    assertFalse(cuckooJedis.hexists("foo", "foo"));
    assertTrue(cuckooJedis.hexists("foo", "bar"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    cuckooJedis.hmset(bfoo, bhash);

    assertFalse(cuckooJedis.hexists(bbar, bfoo));
    assertFalse(cuckooJedis.hexists(bfoo, bfoo));
    assertTrue(cuckooJedis.hexists(bfoo, bbar));

  }

  @Test
  public void hdel() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    cuckooJedis.hmset("foo", hash);

    assertEquals(0, cuckooJedis.hdel("bar", "foo").intValue());
    assertEquals(0, cuckooJedis.hdel("foo", "foo").intValue());
    assertEquals(1, cuckooJedis.hdel("foo", "bar").intValue());
    assertNull(cuckooJedis.hget("foo", "bar"));

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    cuckooJedis.hmset(bfoo, bhash);

    assertEquals(0, cuckooJedis.hdel(bbar, bfoo).intValue());
    assertEquals(0, cuckooJedis.hdel(bfoo, bfoo).intValue());
    assertEquals(1, cuckooJedis.hdel(bfoo, bbar).intValue());
    assertNull(cuckooJedis.hget(bfoo, bbar));

  }

  @Test
  public void hlen() {
    Map<String, String> hash = new HashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    cuckooJedis.hmset("foo", hash);

    assertEquals(0, cuckooJedis.hlen("bar").intValue());
    assertEquals(2, cuckooJedis.hlen("foo").intValue());

    // Binary
    Map<byte[], byte[]> bhash = new HashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    cuckooJedis.hmset(bfoo, bhash);

    assertEquals(0, cuckooJedis.hlen(bbar).intValue());
    assertEquals(2, cuckooJedis.hlen(bfoo).intValue());

  }

  @Test
  public void hkeys() {
    Map<String, String> hash = new LinkedHashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    cuckooJedis.hmset("foo", hash);

    Set<String> keys = cuckooJedis.hkeys("foo");
    Set<String> expected = new LinkedHashSet<String>();
    expected.add("bar");
    expected.add("car");
    assertEquals(expected, keys);

    // Binary
    Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    cuckooJedis.hmset(bfoo, bhash);

    Set<byte[]> bkeys = cuckooJedis.hkeys(bfoo);
    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bbar);
    bexpected.add(bcar);
    assertByteArraySetEquals(bexpected, bkeys);
  }

  @Test
  public void hvals() {
    Map<String, String> hash = new LinkedHashMap<String, String>();
    hash.put("bar", "car");
    hash.put("car", "bar");
    cuckooJedis.hmset("foo", hash);

    List<String> vals = cuckooJedis.hvals("foo");
    assertEquals(2, vals.size());
    assertTrue(vals.contains("bar"));
    assertTrue(vals.contains("car"));

    // Binary
    Map<byte[], byte[]> bhash = new LinkedHashMap<byte[], byte[]>();
    bhash.put(bbar, bcar);
    bhash.put(bcar, bbar);
    cuckooJedis.hmset(bfoo, bhash);

    List<byte[]> bvals = cuckooJedis.hvals(bfoo);

    assertEquals(2, bvals.size());
    assertTrue(arrayContains(bvals, bbar));
    assertTrue(arrayContains(bvals, bcar));
  }

  @Test
  public void hgetAll() {
    Map<String, String> h = new HashMap<String, String>();
    h.put("bar", "car");
    h.put("car", "bar");
    cuckooJedis.hmset("foo", h);

    Map<String, String> hash = cuckooJedis.hgetAll("foo");
    assertEquals(2, hash.size());
    assertEquals("car", hash.get("bar"));
    assertEquals("bar", hash.get("car"));

    // Binary
    Map<byte[], byte[]> bh = new HashMap<byte[], byte[]>();
    bh.put(bbar, bcar);
    bh.put(bcar, bbar);
    cuckooJedis.hmset(bfoo, bh);
    Map<byte[], byte[]> bhash = cuckooJedis.hgetAll(bfoo);

    assertEquals(2, bhash.size());
    assertArrayEquals(bcar, bhash.get(bbar));
    assertArrayEquals(bbar, bhash.get(bcar));
  }

  @Test
  public void hgetAllPipeline() {
    Map<byte[], byte[]> bh = new HashMap<byte[], byte[]>();
    bh.put(bbar, bcar);
    bh.put(bcar, bbar);
    cuckooJedis.hmset(bfoo, bh);
    Pipeline pipeline = cuckooJedis.pipelined();
    Response<Map<byte[], byte[]>> bhashResponse = pipeline.hgetAll(bfoo);
    pipeline.sync();
    Map<byte[], byte[]> bhash = bhashResponse.get();

    assertEquals(2, bhash.size());
    assertArrayEquals(bcar, bhash.get(bbar));
    assertArrayEquals(bbar, bhash.get(bcar));
  }

  @Test
  public void hscan() {
    cuckooJedis.hset("foo", "b", "b");
    cuckooJedis.hset("foo", "a", "a");

    ScanResult<Map.Entry<String, String>> result = cuckooJedis.hscan("foo", SCAN_POINTER_START);

    assertEquals(SCAN_POINTER_START, result.getCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    cuckooJedis.hset(bfoo, bbar, bcar);

    ScanResult<Map.Entry<byte[], byte[]>> bResult = cuckooJedis.hscan(bfoo, SCAN_POINTER_START_BINARY);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void hscanMatch() {
    ScanParams params = new ScanParams();
    params.match("a*");

    cuckooJedis.hset("foo", "b", "b");
    cuckooJedis.hset("foo", "a", "a");
    cuckooJedis.hset("foo", "aa", "aa");
    ScanResult<Map.Entry<String, String>> result = cuckooJedis.hscan("foo", SCAN_POINTER_START, params);

    assertEquals(SCAN_POINTER_START, result.getCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.match(bbarstar);

    cuckooJedis.hset(bfoo, bbar, bcar);
    cuckooJedis.hset(bfoo, bbar1, bcar);
    cuckooJedis.hset(bfoo, bbar2, bcar);
    cuckooJedis.hset(bfoo, bbar3, bcar);

    ScanResult<Map.Entry<byte[], byte[]>> bResult = cuckooJedis.hscan(bfoo, SCAN_POINTER_START_BINARY,
      params);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void hscanCount() {
    ScanParams params = new ScanParams();
    params.count(2);

    for (int i = 0; i < 10; i++) {
      cuckooJedis.hset("foo", "a" + i, "a" + i);
    }

    ScanResult<Map.Entry<String, String>> result = cuckooJedis.hscan("foo", SCAN_POINTER_START, params);

    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.count(2);

    cuckooJedis.hset(bfoo, bbar, bcar);
    cuckooJedis.hset(bfoo, bbar1, bcar);
    cuckooJedis.hset(bfoo, bbar2, bcar);
    cuckooJedis.hset(bfoo, bbar3, bcar);

    ScanResult<Map.Entry<byte[], byte[]>> bResult = cuckooJedis.hscan(bfoo, SCAN_POINTER_START_BINARY,
      params);

    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void testHstrLen_EmptyHash() {
    Long response = cuckooJedis.hstrlen("myhash", "k1");
    assertEquals(0l, response.longValue());
  }

  @Test
  public void testHstrLen() {
    Map<String, String> values = new HashMap<>();
    values.put("key", "value");
    cuckooJedis.hmset("myhash", values);
    Long response = cuckooJedis.hstrlen("myhash", "key");
    assertEquals(5l, response.longValue());

  }

  @Test
  public void testBinaryHstrLen() {
    Map<byte[], byte[]> values = new HashMap<>();
    values.put(bbar, bcar);
    cuckooJedis.hmset(bfoo, values);
    Long response = cuckooJedis.hstrlen(bfoo, bbar);
    assertEquals(4l, response.longValue());
  }

}
