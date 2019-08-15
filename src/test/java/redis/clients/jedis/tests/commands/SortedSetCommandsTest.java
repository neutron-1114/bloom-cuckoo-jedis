package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;
import static redis.clients.jedis.tests.utils.AssertUtil.assertByteArraySetEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;
import redis.clients.jedis.util.SafeEncoder;

public class SortedSetCommandsTest extends CuckooJedisCommandTestBase {
  final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
  final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
  final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };
  final byte[] ba = { 0x0A };
  final byte[] bb = { 0x0B };
  final byte[] bc = { 0x0C };
  final byte[] bInclusiveB = { 0x5B, 0x0B };
  final byte[] bExclusiveC = { 0x28, 0x0C };
  final byte[] bLexMinusInf = { 0x2D };
  final byte[] bLexPlusInf = { 0x2B };

  final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, 0x0A };
  final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, 0x0B };
  final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, 0x0C };
  final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };

  @Test
  public void zadd() {
    long status = cuckooJedis.zadd("foo", 1d, "a");
    assertEquals(1, status);

    status = cuckooJedis.zadd("foo", 10d, "b");
    assertEquals(1, status);

    status = cuckooJedis.zadd("foo", 0.1d, "c");
    assertEquals(1, status);

    status = cuckooJedis.zadd("foo", 2d, "a");
    assertEquals(0, status);

    // Binary
    long bstatus = cuckooJedis.zadd(bfoo, 1d, ba);
    assertEquals(1, bstatus);

    bstatus = cuckooJedis.zadd(bfoo, 10d, bb);
    assertEquals(1, bstatus);

    bstatus = cuckooJedis.zadd(bfoo, 0.1d, bc);
    assertEquals(1, bstatus);

    bstatus = cuckooJedis.zadd(bfoo, 2d, ba);
    assertEquals(0, bstatus);

  }

  @Test
  public void zaddWithParams() {
    cuckooJedis.del("foo");

    // xx: never add new member
    long status = cuckooJedis.zadd("foo", 1d, "a", ZAddParams.zAddParams().xx());
    assertEquals(0L, status);

    cuckooJedis.zadd("foo", 1d, "a");
    // nx: never update current member
    status = cuckooJedis.zadd("foo", 2d, "a", ZAddParams.zAddParams().nx());
    assertEquals(0L, status);
    assertEquals(Double.valueOf(1d), cuckooJedis.zscore("foo", "a"));

    Map<String, Double> scoreMembers = new HashMap<String, Double>();
    scoreMembers.put("a", 2d);
    scoreMembers.put("b", 1d);
    // ch: return count of members not only added, but also updated
    status = cuckooJedis.zadd("foo", scoreMembers, ZAddParams.zAddParams().ch());
    assertEquals(2L, status);

    // binary
    cuckooJedis.del(bfoo);

    // xx: never add new member
    status = cuckooJedis.zadd(bfoo, 1d, ba, ZAddParams.zAddParams().xx());
    assertEquals(0L, status);

    cuckooJedis.zadd(bfoo, 1d, ba);
    // nx: never update current member
    status = cuckooJedis.zadd(bfoo, 2d, ba, ZAddParams.zAddParams().nx());
    assertEquals(0L, status);
    assertEquals(Double.valueOf(1d), cuckooJedis.zscore(bfoo, ba));

    Map<byte[], Double> binaryScoreMembers = new HashMap<byte[], Double>();
    binaryScoreMembers.put(ba, 2d);
    binaryScoreMembers.put(bb, 1d);
    // ch: return count of members not only added, but also updated
    status = cuckooJedis.zadd(bfoo, binaryScoreMembers, ZAddParams.zAddParams().ch());
    assertEquals(2L, status);
  }

  @Test
  public void zrange() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("c");
    expected.add("a");

    Set<String> range = cuckooJedis.zrange("foo", 0, 1);
    assertEquals(expected, range);

    expected.add("b");
    range = cuckooJedis.zrange("foo", 0, 100);
    assertEquals(expected, range);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);
    bexpected.add(ba);

    Set<byte[]> brange = cuckooJedis.zrange(bfoo, 0, 1);
    assertByteArraySetEquals(bexpected, brange);

    bexpected.add(bb);
    brange = cuckooJedis.zrange(bfoo, 0, 100);
    assertByteArraySetEquals(bexpected, brange);

  }

  @Test
  public void zrangeByLex() {
    cuckooJedis.zadd("foo", 1, "aa");
    cuckooJedis.zadd("foo", 1, "c");
    cuckooJedis.zadd("foo", 1, "bb");
    cuckooJedis.zadd("foo", 1, "d");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("bb");
    expected.add("c");

    // exclusive aa ~ inclusive c
    assertEquals(expected, cuckooJedis.zrangeByLex("foo", "(aa", "[c"));

    expected.clear();
    expected.add("bb");
    expected.add("c");

    // with LIMIT
    assertEquals(expected, cuckooJedis.zrangeByLex("foo", "-", "+", 1, 2));
  }

  @Test
  public void zrangeByLexBinary() {
    // binary
    cuckooJedis.zadd(bfoo, 1, ba);
    cuckooJedis.zadd(bfoo, 1, bc);
    cuckooJedis.zadd(bfoo, 1, bb);

    Set<byte[]> bExpected = new LinkedHashSet<byte[]>();
    bExpected.add(bb);

    assertByteArraySetEquals(bExpected, cuckooJedis.zrangeByLex(bfoo, bInclusiveB, bExclusiveC));

    bExpected.clear();
    bExpected.add(ba);
    bExpected.add(bb);

    // with LIMIT
    assertByteArraySetEquals(bExpected, cuckooJedis.zrangeByLex(bfoo, bLexMinusInf, bLexPlusInf, 0, 2));
  }

  @Test
  public void zrevrangeByLex() {
    cuckooJedis.zadd("foo", 1, "aa");
    cuckooJedis.zadd("foo", 1, "c");
    cuckooJedis.zadd("foo", 1, "bb");
    cuckooJedis.zadd("foo", 1, "d");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("c");
    expected.add("bb");

    // exclusive aa ~ inclusive c
    assertEquals(expected, cuckooJedis.zrevrangeByLex("foo", "[c", "(aa"));

    expected.clear();
    expected.add("c");
    expected.add("bb");

    // with LIMIT
    assertEquals(expected, cuckooJedis.zrevrangeByLex("foo", "+", "-", 1, 2));
  }

  @Test
  public void zrevrangeByLexBinary() {
    // binary
    cuckooJedis.zadd(bfoo, 1, ba);
    cuckooJedis.zadd(bfoo, 1, bc);
    cuckooJedis.zadd(bfoo, 1, bb);

    Set<byte[]> bExpected = new LinkedHashSet<byte[]>();
    bExpected.add(bb);

    assertByteArraySetEquals(bExpected, cuckooJedis.zrevrangeByLex(bfoo, bExclusiveC, bInclusiveB));

    bExpected.clear();
    bExpected.add(bb);
    bExpected.add(ba);

    // with LIMIT
    assertByteArraySetEquals(bExpected, cuckooJedis.zrevrangeByLex(bfoo, bLexPlusInf, bLexMinusInf, 0, 2));
  }

  @Test
  public void zrevrange() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("b");
    expected.add("a");

    Set<String> range = cuckooJedis.zrevrange("foo", 0, 1);
    assertEquals(expected, range);

    expected.add("c");
    range = cuckooJedis.zrevrange("foo", 0, 100);
    assertEquals(expected, range);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(ba);

    Set<byte[]> brange = cuckooJedis.zrevrange(bfoo, 0, 1);
    assertByteArraySetEquals(bexpected, brange);

    bexpected.add(bc);
    brange = cuckooJedis.zrevrange(bfoo, 0, 100);
    assertByteArraySetEquals(bexpected, brange);

  }

  @Test
  public void zrem() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 2d, "b");

    long status = cuckooJedis.zrem("foo", "a");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("b");

    assertEquals(1, status);
    assertEquals(expected, cuckooJedis.zrange("foo", 0, 100));

    status = cuckooJedis.zrem("foo", "bar");

    assertEquals(0, status);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 2d, bb);

    long bstatus = cuckooJedis.zrem(bfoo, ba);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);

    assertEquals(1, bstatus);
    assertByteArraySetEquals(bexpected, cuckooJedis.zrange(bfoo, 0, 100));

    bstatus = cuckooJedis.zrem(bfoo, bbar);

    assertEquals(0, bstatus);

  }

  @Test
  public void zincrby() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 2d, "b");

    double score = cuckooJedis.zincrby("foo", 2d, "a");

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("a");
    expected.add("b");

    assertEquals(3d, score, 0);
    assertEquals(expected, cuckooJedis.zrange("foo", 0, 100));

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 2d, bb);

    double bscore = cuckooJedis.zincrby(bfoo, 2d, ba);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(ba);

    assertEquals(3d, bscore, 0);
    assertByteArraySetEquals(bexpected, cuckooJedis.zrange(bfoo, 0, 100));

  }

  @Test
  public void zincrbyWithParams() {
    cuckooJedis.del("foo");

    // xx: never add new member
    Double score = cuckooJedis.zincrby("foo", 2d, "a", ZIncrByParams.zIncrByParams().xx());
    assertNull(score);

    cuckooJedis.zadd("foo", 2d, "a");

    // nx: never update current member
    score = cuckooJedis.zincrby("foo", 1d, "a", ZIncrByParams.zIncrByParams().nx());
    assertNull(score);
    assertEquals(Double.valueOf(2d), cuckooJedis.zscore("foo", "a"));

    // Binary

    cuckooJedis.del(bfoo);

    // xx: never add new member
    score = cuckooJedis.zincrby(bfoo, 2d, ba, ZIncrByParams.zIncrByParams().xx());
    assertNull(score);

    cuckooJedis.zadd(bfoo, 2d, ba);

    // nx: never update current member
    score = cuckooJedis.zincrby(bfoo, 1d, ba, ZIncrByParams.zIncrByParams().nx());
    assertNull(score);
    assertEquals(Double.valueOf(2d), cuckooJedis.zscore(bfoo, ba));
  }

  @Test
  public void zrank() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 2d, "b");

    long rank = cuckooJedis.zrank("foo", "a");
    assertEquals(0, rank);

    rank = cuckooJedis.zrank("foo", "b");
    assertEquals(1, rank);

    assertNull(cuckooJedis.zrank("car", "b"));

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 2d, bb);

    long brank = cuckooJedis.zrank(bfoo, ba);
    assertEquals(0, brank);

    brank = cuckooJedis.zrank(bfoo, bb);
    assertEquals(1, brank);

    assertNull(cuckooJedis.zrank(bcar, bb));

  }

  @Test
  public void zrevrank() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 2d, "b");

    long rank = cuckooJedis.zrevrank("foo", "a");
    assertEquals(1, rank);

    rank = cuckooJedis.zrevrank("foo", "b");
    assertEquals(0, rank);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 2d, bb);

    long brank = cuckooJedis.zrevrank(bfoo, ba);
    assertEquals(1, brank);

    brank = cuckooJedis.zrevrank(bfoo, bb);
    assertEquals(0, brank);

  }

  @Test
  public void zrangeWithScores() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 0.1d));
    expected.add(new Tuple("a", 2d));

    Set<Tuple> range = cuckooJedis.zrangeWithScores("foo", 0, 1);
    assertEquals(expected, range);

    expected.add(new Tuple("b", 10d));
    range = cuckooJedis.zrangeWithScores("foo", 0, 100);
    assertEquals(expected, range);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));
    bexpected.add(new Tuple(ba, 2d));

    Set<Tuple> brange = cuckooJedis.zrangeWithScores(bfoo, 0, 1);
    assertEquals(bexpected, brange);

    bexpected.add(new Tuple(bb, 10d));
    brange = cuckooJedis.zrangeWithScores(bfoo, 0, 100);
    assertEquals(bexpected, brange);

  }

  @Test
  public void zrevrangeWithScores() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("b", 10d));
    expected.add(new Tuple("a", 2d));

    Set<Tuple> range = cuckooJedis.zrevrangeWithScores("foo", 0, 1);
    assertEquals(expected, range);

    expected.add(new Tuple("c", 0.1d));
    range = cuckooJedis.zrevrangeWithScores("foo", 0, 100);
    assertEquals(expected, range);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bb, 10d));
    bexpected.add(new Tuple(ba, 2d));

    Set<Tuple> brange = cuckooJedis.zrevrangeWithScores(bfoo, 0, 1);
    assertEquals(bexpected, brange);

    bexpected.add(new Tuple(bc, 0.1d));
    brange = cuckooJedis.zrevrangeWithScores(bfoo, 0, 100);
    assertEquals(bexpected, brange);

  }

  @Test
  public void zcard() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    long size = cuckooJedis.zcard("foo");
    assertEquals(3, size);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    long bsize = cuckooJedis.zcard(bfoo);
    assertEquals(3, bsize);

  }

  @Test
  public void zscore() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    Double score = cuckooJedis.zscore("foo", "b");
    assertEquals((Double) 10d, score);

    score = cuckooJedis.zscore("foo", "c");
    assertEquals((Double) 0.1d, score);

    score = cuckooJedis.zscore("foo", "s");
    assertNull(score);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    Double bscore = cuckooJedis.zscore(bfoo, bb);
    assertEquals((Double) 10d, bscore);

    bscore = cuckooJedis.zscore(bfoo, bc);
    assertEquals((Double) 0.1d, bscore);

    bscore = cuckooJedis.zscore(bfoo, SafeEncoder.encode("s"));
    assertNull(bscore);

  }

  @Test
  public void zcount() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    long result = cuckooJedis.zcount("foo", 0.01d, 2.1d);

    assertEquals(2, result);

    result = cuckooJedis.zcount("foo", "(0.01", "+inf");

    assertEquals(3, result);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    long bresult = cuckooJedis.zcount(bfoo, 0.01d, 2.1d);

    assertEquals(2, bresult);

    bresult = cuckooJedis.zcount(bfoo, SafeEncoder.encode("(0.01"), SafeEncoder.encode("+inf"));

    assertEquals(3, bresult);
  }

  @Test
  public void zlexcount() {
    cuckooJedis.zadd("foo", 1, "a");
    cuckooJedis.zadd("foo", 1, "b");
    cuckooJedis.zadd("foo", 1, "c");
    cuckooJedis.zadd("foo", 1, "aa");

    long result = cuckooJedis.zlexcount("foo", "[aa", "(c");
    assertEquals(2, result);

    result = cuckooJedis.zlexcount("foo", "-", "+");
    assertEquals(4, result);

    result = cuckooJedis.zlexcount("foo", "-", "(c");
    assertEquals(3, result);

    result = cuckooJedis.zlexcount("foo", "[aa", "+");
    assertEquals(3, result);
  }

  @Test
  public void zlexcountBinary() {
    // Binary
    cuckooJedis.zadd(bfoo, 1, ba);
    cuckooJedis.zadd(bfoo, 1, bc);
    cuckooJedis.zadd(bfoo, 1, bb);

    long result = cuckooJedis.zlexcount(bfoo, bInclusiveB, bExclusiveC);
    assertEquals(1, result);

    result = cuckooJedis.zlexcount(bfoo, bLexMinusInf, bLexPlusInf);
    assertEquals(3, result);
  }

  @Test
  public void zrangebyscore() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    Set<String> range = cuckooJedis.zrangeByScore("foo", 0d, 2d);

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("c");
    expected.add("a");

    assertEquals(expected, range);

    range = cuckooJedis.zrangeByScore("foo", 0d, 2d, 0, 1);

    expected = new LinkedHashSet<String>();
    expected.add("c");

    assertEquals(expected, range);

    range = cuckooJedis.zrangeByScore("foo", 0d, 2d, 1, 1);
    Set<String> range2 = cuckooJedis.zrangeByScore("foo", "-inf", "(2");
    assertEquals(expected, range2);

    expected = new LinkedHashSet<String>();
    expected.add("a");

    assertEquals(expected, range);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    Set<byte[]> brange = cuckooJedis.zrangeByScore(bfoo, 0d, 2d);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);
    bexpected.add(ba);

    assertByteArraySetEquals(bexpected, brange);

    brange = cuckooJedis.zrangeByScore(bfoo, 0d, 2d, 0, 1);

    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);

    assertByteArraySetEquals(bexpected, brange);

    brange = cuckooJedis.zrangeByScore(bfoo, 0d, 2d, 1, 1);
    Set<byte[]> brange2 = cuckooJedis.zrangeByScore(bfoo, SafeEncoder.encode("-inf"),
      SafeEncoder.encode("(2"));
    assertByteArraySetEquals(bexpected, brange2);

    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(ba);

    assertByteArraySetEquals(bexpected, brange);

  }

  @Test
  public void zrevrangebyscore() {
    cuckooJedis.zadd("foo", 1.0d, "a");
    cuckooJedis.zadd("foo", 2.0d, "b");
    cuckooJedis.zadd("foo", 3.0d, "c");
    cuckooJedis.zadd("foo", 4.0d, "d");
    cuckooJedis.zadd("foo", 5.0d, "e");

    Set<String> range = cuckooJedis.zrevrangeByScore("foo", 3d, Double.NEGATIVE_INFINITY, 0, 1);
    Set<String> expected = new LinkedHashSet<String>();
    expected.add("c");

    assertEquals(expected, range);

    range = cuckooJedis.zrevrangeByScore("foo", 3.5d, Double.NEGATIVE_INFINITY, 0, 2);
    expected = new LinkedHashSet<String>();
    expected.add("c");
    expected.add("b");

    assertEquals(expected, range);

    range = cuckooJedis.zrevrangeByScore("foo", 3.5d, Double.NEGATIVE_INFINITY, 1, 1);
    expected = new LinkedHashSet<String>();
    expected.add("b");

    assertEquals(expected, range);

    range = cuckooJedis.zrevrangeByScore("foo", 4d, 2d);
    expected = new LinkedHashSet<String>();
    expected.add("d");
    expected.add("c");
    expected.add("b");

    assertEquals(expected, range);

    range = cuckooJedis.zrevrangeByScore("foo", "+inf", "(4");
    expected = new LinkedHashSet<String>();
    expected.add("e");

    assertEquals(expected, range);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    Set<byte[]> brange = cuckooJedis.zrevrangeByScore(bfoo, 2d, 0d);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);
    bexpected.add(ba);

    assertByteArraySetEquals(bexpected, brange);

    brange = cuckooJedis.zrevrangeByScore(bfoo, 2d, 0d, 0, 1);

    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(ba);

    assertByteArraySetEquals(bexpected, brange);

    Set<byte[]> brange2 = cuckooJedis.zrevrangeByScore(bfoo, SafeEncoder.encode("+inf"),
      SafeEncoder.encode("(2"));

    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);

    assertByteArraySetEquals(bexpected, brange2);

    brange = cuckooJedis.zrevrangeByScore(bfoo, 2d, 0d, 1, 1);
    bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bc);

    assertByteArraySetEquals(bexpected, brange);
  }

  @Test
  public void zrangebyscoreWithScores() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    Set<Tuple> range = cuckooJedis.zrangeByScoreWithScores("foo", 0d, 2d);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 0.1d));
    expected.add(new Tuple("a", 2d));

    assertEquals(expected, range);

    range = cuckooJedis.zrangeByScoreWithScores("foo", 0d, 2d, 0, 1);

    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 0.1d));

    assertEquals(expected, range);

    range = cuckooJedis.zrangeByScoreWithScores("foo", 0d, 2d, 1, 1);

    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("a", 2d));

    assertEquals(expected, range);

    // Binary

    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    Set<Tuple> brange = cuckooJedis.zrangeByScoreWithScores(bfoo, 0d, 2d);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));
    bexpected.add(new Tuple(ba, 2d));

    assertEquals(bexpected, brange);

    brange = cuckooJedis.zrangeByScoreWithScores(bfoo, 0d, 2d, 0, 1);

    bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));

    assertEquals(bexpected, brange);

    brange = cuckooJedis.zrangeByScoreWithScores(bfoo, 0d, 2d, 1, 1);

    bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(ba, 2d));

    assertEquals(bexpected, brange);

  }

  @Test
  public void zrevrangebyscoreWithScores() {
    cuckooJedis.zadd("foo", 1.0d, "a");
    cuckooJedis.zadd("foo", 2.0d, "b");
    cuckooJedis.zadd("foo", 3.0d, "c");
    cuckooJedis.zadd("foo", 4.0d, "d");
    cuckooJedis.zadd("foo", 5.0d, "e");

    Set<Tuple> range = cuckooJedis.zrevrangeByScoreWithScores("foo", 3d, Double.NEGATIVE_INFINITY, 0, 1);
    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 3.0d));

    assertEquals(expected, range);

    range = cuckooJedis.zrevrangeByScoreWithScores("foo", 3.5d, Double.NEGATIVE_INFINITY, 0, 2);
    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("c", 3.0d));
    expected.add(new Tuple("b", 2.0d));

    assertEquals(expected, range);

    range = cuckooJedis.zrevrangeByScoreWithScores("foo", 3.5d, Double.NEGATIVE_INFINITY, 1, 1);
    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("b", 2.0d));

    assertEquals(expected, range);

    range = cuckooJedis.zrevrangeByScoreWithScores("foo", 4d, 2d);
    expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("d", 4.0d));
    expected.add(new Tuple("c", 3.0d));
    expected.add(new Tuple("b", 2.0d));

    assertEquals(expected, range);

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    Set<Tuple> brange = cuckooJedis.zrevrangeByScoreWithScores(bfoo, 2d, 0d);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));
    bexpected.add(new Tuple(ba, 2d));

    assertEquals(bexpected, brange);

    brange = cuckooJedis.zrevrangeByScoreWithScores(bfoo, 2d, 0d, 0, 1);

    bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(ba, 2d));

    assertEquals(bexpected, brange);

    brange = cuckooJedis.zrevrangeByScoreWithScores(bfoo, 2d, 0d, 1, 1);

    bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bc, 0.1d));

    assertEquals(bexpected, brange);
  }

  @Test
  public void zremrangeByRank() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    long result = cuckooJedis.zremrangeByRank("foo", 0, 0);

    assertEquals(1, result);

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("a");
    expected.add("b");

    assertEquals(expected, cuckooJedis.zrange("foo", 0, 100));

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    long bresult = cuckooJedis.zremrangeByRank(bfoo, 0, 0);

    assertEquals(1, bresult);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(ba);
    bexpected.add(bb);

    assertByteArraySetEquals(bexpected, cuckooJedis.zrange(bfoo, 0, 100));

  }

  @Test
  public void zremrangeByScore() {
    cuckooJedis.zadd("foo", 1d, "a");
    cuckooJedis.zadd("foo", 10d, "b");
    cuckooJedis.zadd("foo", 0.1d, "c");
    cuckooJedis.zadd("foo", 2d, "a");

    long result = cuckooJedis.zremrangeByScore("foo", 0, 2);

    assertEquals(2, result);

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("b");

    assertEquals(expected, cuckooJedis.zrange("foo", 0, 100));

    // Binary
    cuckooJedis.zadd(bfoo, 1d, ba);
    cuckooJedis.zadd(bfoo, 10d, bb);
    cuckooJedis.zadd(bfoo, 0.1d, bc);
    cuckooJedis.zadd(bfoo, 2d, ba);

    long bresult = cuckooJedis.zremrangeByScore(bfoo, 0, 2);

    assertEquals(2, bresult);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(bb);

    assertByteArraySetEquals(bexpected, cuckooJedis.zrange(bfoo, 0, 100));
  }

  @Test
  public void zremrangeByLex() {
    cuckooJedis.zadd("foo", 1, "a");
    cuckooJedis.zadd("foo", 1, "b");
    cuckooJedis.zadd("foo", 1, "c");
    cuckooJedis.zadd("foo", 1, "aa");

    long result = cuckooJedis.zremrangeByLex("foo", "[aa", "(c");

    assertEquals(2, result);

    Set<String> expected = new LinkedHashSet<String>();
    expected.add("a");
    expected.add("c");

    assertEquals(expected, cuckooJedis.zrangeByLex("foo", "-", "+"));
  }

  @Test
  public void zremrangeByLexBinary() {
    cuckooJedis.zadd(bfoo, 1, ba);
    cuckooJedis.zadd(bfoo, 1, bc);
    cuckooJedis.zadd(bfoo, 1, bb);

    long bresult = cuckooJedis.zremrangeByLex(bfoo, bInclusiveB, bExclusiveC);

    assertEquals(1, bresult);

    Set<byte[]> bexpected = new LinkedHashSet<byte[]>();
    bexpected.add(ba);
    bexpected.add(bc);

    assertByteArraySetEquals(bexpected, cuckooJedis.zrangeByLex(bfoo, bLexMinusInf, bLexPlusInf));
  }

  @Test
  public void zunionstore() {
    cuckooJedis.zadd("foo", 1, "a");
    cuckooJedis.zadd("foo", 2, "b");
    cuckooJedis.zadd("bar", 2, "a");
    cuckooJedis.zadd("bar", 2, "b");

    long result = cuckooJedis.zunionstore("dst", "foo", "bar");

    assertEquals(2, result);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("b", new Double(4)));
    expected.add(new Tuple("a", new Double(3)));

    assertEquals(expected, cuckooJedis.zrangeWithScores("dst", 0, 100));

    // Binary
    cuckooJedis.zadd(bfoo, 1, ba);
    cuckooJedis.zadd(bfoo, 2, bb);
    cuckooJedis.zadd(bbar, 2, ba);
    cuckooJedis.zadd(bbar, 2, bb);

    long bresult = cuckooJedis.zunionstore(SafeEncoder.encode("dst"), bfoo, bbar);

    assertEquals(2, bresult);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bb, new Double(4)));
    bexpected.add(new Tuple(ba, new Double(3)));

    assertEquals(bexpected, cuckooJedis.zrangeWithScores(SafeEncoder.encode("dst"), 0, 100));
  }

  @Test
  public void zunionstoreParams() {
    cuckooJedis.zadd("foo", 1, "a");
    cuckooJedis.zadd("foo", 2, "b");
    cuckooJedis.zadd("bar", 2, "a");
    cuckooJedis.zadd("bar", 2, "b");

    ZParams params = new ZParams();
    params.weights(2, 2.5);
    params.aggregate(ZParams.Aggregate.SUM);
    long result = cuckooJedis.zunionstore("dst", params, "foo", "bar");

    assertEquals(2, result);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("b", new Double(9)));
    expected.add(new Tuple("a", new Double(7)));

    assertEquals(expected, cuckooJedis.zrangeWithScores("dst", 0, 100));

    // Binary
    cuckooJedis.zadd(bfoo, 1, ba);
    cuckooJedis.zadd(bfoo, 2, bb);
    cuckooJedis.zadd(bbar, 2, ba);
    cuckooJedis.zadd(bbar, 2, bb);

    ZParams bparams = new ZParams();
    bparams.weights(2, 2.5);
    bparams.aggregate(ZParams.Aggregate.SUM);
    long bresult = cuckooJedis.zunionstore(SafeEncoder.encode("dst"), bparams, bfoo, bbar);

    assertEquals(2, bresult);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(bb, new Double(9)));
    bexpected.add(new Tuple(ba, new Double(7)));

    assertEquals(bexpected, cuckooJedis.zrangeWithScores(SafeEncoder.encode("dst"), 0, 100));
  }

  @Test
  public void zinterstore() {
    cuckooJedis.zadd("foo", 1, "a");
    cuckooJedis.zadd("foo", 2, "b");
    cuckooJedis.zadd("bar", 2, "a");

    long result = cuckooJedis.zinterstore("dst", "foo", "bar");

    assertEquals(1, result);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("a", new Double(3)));

    assertEquals(expected, cuckooJedis.zrangeWithScores("dst", 0, 100));

    // Binary
    cuckooJedis.zadd(bfoo, 1, ba);
    cuckooJedis.zadd(bfoo, 2, bb);
    cuckooJedis.zadd(bbar, 2, ba);

    long bresult = cuckooJedis.zinterstore(SafeEncoder.encode("dst"), bfoo, bbar);

    assertEquals(1, bresult);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(ba, new Double(3)));

    assertEquals(bexpected, cuckooJedis.zrangeWithScores(SafeEncoder.encode("dst"), 0, 100));
  }

  @Test
  public void zintertoreParams() {
    cuckooJedis.zadd("foo", 1, "a");
    cuckooJedis.zadd("foo", 2, "b");
    cuckooJedis.zadd("bar", 2, "a");

    ZParams params = new ZParams();
    params.weights(2, 2.5);
    params.aggregate(ZParams.Aggregate.SUM);
    long result = cuckooJedis.zinterstore("dst", params, "foo", "bar");

    assertEquals(1, result);

    Set<Tuple> expected = new LinkedHashSet<Tuple>();
    expected.add(new Tuple("a", new Double(7)));

    assertEquals(expected, cuckooJedis.zrangeWithScores("dst", 0, 100));

    // Binary
    cuckooJedis.zadd(bfoo, 1, ba);
    cuckooJedis.zadd(bfoo, 2, bb);
    cuckooJedis.zadd(bbar, 2, ba);

    ZParams bparams = new ZParams();
    bparams.weights(2, 2.5);
    bparams.aggregate(ZParams.Aggregate.SUM);
    long bresult = cuckooJedis.zinterstore(SafeEncoder.encode("dst"), bparams, bfoo, bbar);

    assertEquals(1, bresult);

    Set<Tuple> bexpected = new LinkedHashSet<Tuple>();
    bexpected.add(new Tuple(ba, new Double(7)));

    assertEquals(bexpected, cuckooJedis.zrangeWithScores(SafeEncoder.encode("dst"), 0, 100));
  }

  @Test
  public void zscan() {
    cuckooJedis.zadd("foo", 1, "a");
    cuckooJedis.zadd("foo", 2, "b");

    ScanResult<Tuple> result = cuckooJedis.zscan("foo", SCAN_POINTER_START);

    assertEquals(SCAN_POINTER_START, result.getCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    cuckooJedis.zadd(bfoo, 1, ba);
    cuckooJedis.zadd(bfoo, 1, bb);

    ScanResult<Tuple> bResult = cuckooJedis.zscan(bfoo, SCAN_POINTER_START_BINARY);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void zscanMatch() {
    ScanParams params = new ScanParams();
    params.match("a*");

    cuckooJedis.zadd("foo", 2, "b");
    cuckooJedis.zadd("foo", 1, "a");
    cuckooJedis.zadd("foo", 11, "aa");
    ScanResult<Tuple> result = cuckooJedis.zscan("foo", SCAN_POINTER_START, params);

    assertEquals(SCAN_POINTER_START, result.getCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.match(bbarstar);

    cuckooJedis.zadd(bfoo, 2, bbar1);
    cuckooJedis.zadd(bfoo, 1, bbar2);
    cuckooJedis.zadd(bfoo, 11, bbar3);
    ScanResult<Tuple> bResult = cuckooJedis.zscan(bfoo, SCAN_POINTER_START_BINARY, params);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());

  }

  @Test
  public void zscanCount() {
    ScanParams params = new ScanParams();
    params.count(2);

    cuckooJedis.zadd("foo", 1, "a1");
    cuckooJedis.zadd("foo", 2, "a2");
    cuckooJedis.zadd("foo", 3, "a3");
    cuckooJedis.zadd("foo", 4, "a4");
    cuckooJedis.zadd("foo", 5, "a5");

    ScanResult<Tuple> result = cuckooJedis.zscan("foo", SCAN_POINTER_START, params);

    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.count(2);

    cuckooJedis.zadd(bfoo, 2, bbar1);
    cuckooJedis.zadd(bfoo, 1, bbar2);
    cuckooJedis.zadd(bfoo, 11, bbar3);

    ScanResult<Tuple> bResult = cuckooJedis.zscan(bfoo, SCAN_POINTER_START_BINARY, params);

    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void infinity() {
    cuckooJedis.zadd("key", Double.POSITIVE_INFINITY, "pos");
    assertEquals(Double.POSITIVE_INFINITY, cuckooJedis.zscore("key", "pos"), 0d);
    cuckooJedis.zadd("key", Double.NEGATIVE_INFINITY, "neg");
    assertEquals(Double.NEGATIVE_INFINITY, cuckooJedis.zscore("key", "neg"), 0d);
    cuckooJedis.zadd("key", 0d, "zero");

    Set<Tuple> set = cuckooJedis.zrangeWithScores("key", 0, -1);
    Iterator<Tuple> itr = set.iterator();
    assertEquals(Double.NEGATIVE_INFINITY, itr.next().getScore(), 0d);
    assertEquals(0d, itr.next().getScore(), 0d);
    assertEquals(Double.POSITIVE_INFINITY, itr.next().getScore(), 0d);
  }
}
