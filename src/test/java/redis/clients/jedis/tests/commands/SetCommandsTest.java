package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;
import static redis.clients.jedis.tests.utils.AssertUtil.assertByteArrayCollectionContainsAll;
import static redis.clients.jedis.tests.utils.AssertUtil.assertByteArraySetEquals;
import static redis.clients.jedis.tests.utils.AssertUtil.assertCollectionContainsAll;
import static redis.clients.jedis.tests.utils.ByteArrayUtil.byteArrayCollectionRemoveAll;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

public class SetCommandsTest extends CuckooJedisCommandTestBase {
  final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
  final byte[] bbar = { 0x05, 0x06, 0x07, 0x08 };
  final byte[] bcar = { 0x09, 0x0A, 0x0B, 0x0C };
  final byte[] ba = { 0x0A };
  final byte[] bb = { 0x0B };
  final byte[] bc = { 0x0C };
  final byte[] bd = { 0x0D };
  final byte[] bx = { 0x42 };

  final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, 0x0A };
  final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, 0x0B };
  final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, 0x0C };
  final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };

  @Test
  public void sadd() {
    long status = cuckooJedis.sadd("foo", "a");
    assertEquals(1, status);

    status = cuckooJedis.sadd("foo", "a");
    assertEquals(0, status);

    long bstatus = cuckooJedis.sadd(bfoo, ba);
    assertEquals(1, bstatus);

    bstatus = cuckooJedis.sadd(bfoo, ba);
    assertEquals(0, bstatus);

  }

  @Test
  public void smembers() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    Set<String> expected = new HashSet<String>();
    expected.add("a");
    expected.add("b");

    Set<String> members = cuckooJedis.smembers("foo");

    assertEquals(expected, members);

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(ba);

    Set<byte[]> bmembers = cuckooJedis.smembers(bfoo);

    assertByteArraySetEquals(bexpected, bmembers);
  }

  @Test
  public void srem() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    long status = cuckooJedis.srem("foo", "a");

    Set<String> expected = new HashSet<String>();
    expected.add("b");

    assertEquals(1, status);
    assertEquals(expected, cuckooJedis.smembers("foo"));

    status = cuckooJedis.srem("foo", "bar");

    assertEquals(0, status);

    // Binary

    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    long bstatus = cuckooJedis.srem(bfoo, ba);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);

    assertEquals(1, bstatus);
    assertByteArraySetEquals(bexpected, cuckooJedis.smembers(bfoo));

    bstatus = cuckooJedis.srem(bfoo, bbar);

    assertEquals(0, bstatus);

  }

  @Test
  public void spop() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    String member = cuckooJedis.spop("foo");

    assertTrue("a".equals(member) || "b".equals(member));
    assertEquals(1, cuckooJedis.smembers("foo").size());

    member = cuckooJedis.spop("bar");
    assertNull(member);

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    byte[] bmember = cuckooJedis.spop(bfoo);

    assertTrue(Arrays.equals(ba, bmember) || Arrays.equals(bb, bmember));
    assertEquals(1, cuckooJedis.smembers(bfoo).size());

    bmember = cuckooJedis.spop(bbar);
    assertNull(bmember);

  }

  @Test
  public void spopWithCount() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");
    cuckooJedis.sadd("foo", "c");

    Set<String> superSet = new HashSet<String>();
    superSet.add("c");
    superSet.add("b");
    superSet.add("a");

    Set<String> members = cuckooJedis.spop("foo", 2);

    assertEquals(2, members.size());
    assertCollectionContainsAll(superSet, members);
    superSet.removeAll(members);

    members = cuckooJedis.spop("foo", 2);
    assertEquals(1, members.size());
    assertEquals(superSet, members);

    assertNull(cuckooJedis.spop("foo", 2));

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);
    cuckooJedis.sadd(bfoo, bc);

    Set<byte[]> bsuperSet = new HashSet<byte[]>();
    bsuperSet.add(bc);
    bsuperSet.add(bb);
    bsuperSet.add(ba);

    Set<byte[]> bmembers = cuckooJedis.spop(bfoo, 2);

    assertEquals(2, bmembers.size());
    assertByteArrayCollectionContainsAll(bsuperSet, bmembers);
    byteArrayCollectionRemoveAll(bsuperSet, bmembers);

    bmembers = cuckooJedis.spop(bfoo, 2);
    assertEquals(1, bmembers.size());
    assertByteArraySetEquals(bsuperSet, bmembers);

    assertNull(cuckooJedis.spop(bfoo, 2));
  }

  @Test
  public void smove() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    cuckooJedis.sadd("bar", "c");

    long status = cuckooJedis.smove("foo", "bar", "a");

    Set<String> expectedSrc = new HashSet<String>();
    expectedSrc.add("b");

    Set<String> expectedDst = new HashSet<String>();
    expectedDst.add("c");
    expectedDst.add("a");

    assertEquals(status, 1);
    assertEquals(expectedSrc, cuckooJedis.smembers("foo"));
    assertEquals(expectedDst, cuckooJedis.smembers("bar"));

    status = cuckooJedis.smove("foo", "bar", "a");

    assertEquals(status, 0);

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    cuckooJedis.sadd(bbar, bc);

    long bstatus = cuckooJedis.smove(bfoo, bbar, ba);

    Set<byte[]> bexpectedSrc = new HashSet<byte[]>();
    bexpectedSrc.add(bb);

    Set<byte[]> bexpectedDst = new HashSet<byte[]>();
    bexpectedDst.add(bc);
    bexpectedDst.add(ba);

    assertEquals(bstatus, 1);
    assertByteArraySetEquals(bexpectedSrc, cuckooJedis.smembers(bfoo));
    assertByteArraySetEquals(bexpectedDst, cuckooJedis.smembers(bbar));

    bstatus = cuckooJedis.smove(bfoo, bbar, ba);
    assertEquals(bstatus, 0);

  }

  @Test
  public void scard() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    long card = cuckooJedis.scard("foo");

    assertEquals(2, card);

    card = cuckooJedis.scard("bar");
    assertEquals(0, card);

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    long bcard = cuckooJedis.scard(bfoo);

    assertEquals(2, bcard);

    bcard = cuckooJedis.scard(bbar);
    assertEquals(0, bcard);

  }

  @Test
  public void sismember() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    assertTrue(cuckooJedis.sismember("foo", "a"));

    assertFalse(cuckooJedis.sismember("foo", "c"));

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    assertTrue(cuckooJedis.sismember(bfoo, ba));

    assertFalse(cuckooJedis.sismember(bfoo, bc));

  }

  @Test
  public void sinter() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    cuckooJedis.sadd("bar", "b");
    cuckooJedis.sadd("bar", "c");

    Set<String> expected = new HashSet<String>();
    expected.add("b");

    Set<String> intersection = cuckooJedis.sinter("foo", "bar");
    assertEquals(expected, intersection);

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    cuckooJedis.sadd(bbar, bb);
    cuckooJedis.sadd(bbar, bc);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);

    Set<byte[]> bintersection = cuckooJedis.sinter(bfoo, bbar);
    assertByteArraySetEquals(bexpected, bintersection);
  }

  @Test
  public void sinterstore() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    cuckooJedis.sadd("bar", "b");
    cuckooJedis.sadd("bar", "c");

    Set<String> expected = new HashSet<String>();
    expected.add("b");

    long status = cuckooJedis.sinterstore("car", "foo", "bar");
    assertEquals(1, status);

    assertEquals(expected, cuckooJedis.smembers("car"));

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    cuckooJedis.sadd(bbar, bb);
    cuckooJedis.sadd(bbar, bc);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);

    long bstatus = cuckooJedis.sinterstore(bcar, bfoo, bbar);
    assertEquals(1, bstatus);

    assertByteArraySetEquals(bexpected, cuckooJedis.smembers(bcar));

  }

  @Test
  public void sunion() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    cuckooJedis.sadd("bar", "b");
    cuckooJedis.sadd("bar", "c");

    Set<String> expected = new HashSet<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");

    Set<String> union = cuckooJedis.sunion("foo", "bar");
    assertEquals(expected, union);

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    cuckooJedis.sadd(bbar, bb);
    cuckooJedis.sadd(bbar, bc);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(bc);
    bexpected.add(ba);

    Set<byte[]> bunion = cuckooJedis.sunion(bfoo, bbar);
    assertByteArraySetEquals(bexpected, bunion);

  }

  @Test
  public void sunionstore() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    cuckooJedis.sadd("bar", "b");
    cuckooJedis.sadd("bar", "c");

    Set<String> expected = new HashSet<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");

    long status = cuckooJedis.sunionstore("car", "foo", "bar");
    assertEquals(3, status);

    assertEquals(expected, cuckooJedis.smembers("car"));

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    cuckooJedis.sadd(bbar, bb);
    cuckooJedis.sadd(bbar, bc);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(bc);
    bexpected.add(ba);

    long bstatus = cuckooJedis.sunionstore(bcar, bfoo, bbar);
    assertEquals(3, bstatus);

    assertByteArraySetEquals(bexpected, cuckooJedis.smembers(bcar));

  }

  @Test
  public void sdiff() {
    cuckooJedis.sadd("foo", "x");
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");
    cuckooJedis.sadd("foo", "c");

    cuckooJedis.sadd("bar", "c");

    cuckooJedis.sadd("car", "a");
    cuckooJedis.sadd("car", "d");

    Set<String> expected = new HashSet<String>();
    expected.add("x");
    expected.add("b");

    Set<String> diff = cuckooJedis.sdiff("foo", "bar", "car");
    assertEquals(expected, diff);

    // Binary
    cuckooJedis.sadd(bfoo, bx);
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);
    cuckooJedis.sadd(bfoo, bc);

    cuckooJedis.sadd(bbar, bc);

    cuckooJedis.sadd(bcar, ba);
    cuckooJedis.sadd(bcar, bd);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bb);
    bexpected.add(bx);

    Set<byte[]> bdiff = cuckooJedis.sdiff(bfoo, bbar, bcar);
    assertByteArraySetEquals(bexpected, bdiff);

  }

  @Test
  public void sdiffstore() {
    cuckooJedis.sadd("foo", "x");
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");
    cuckooJedis.sadd("foo", "c");

    cuckooJedis.sadd("bar", "c");

    cuckooJedis.sadd("car", "a");
    cuckooJedis.sadd("car", "d");

    Set<String> expected = new HashSet<String>();
    expected.add("d");
    expected.add("a");

    long status = cuckooJedis.sdiffstore("tar", "foo", "bar", "car");
    assertEquals(2, status);
    assertEquals(expected, cuckooJedis.smembers("car"));

    // Binary
    cuckooJedis.sadd(bfoo, bx);
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);
    cuckooJedis.sadd(bfoo, bc);

    cuckooJedis.sadd(bbar, bc);

    cuckooJedis.sadd(bcar, ba);
    cuckooJedis.sadd(bcar, bd);

    Set<byte[]> bexpected = new HashSet<byte[]>();
    bexpected.add(bd);
    bexpected.add(ba);

    long bstatus = cuckooJedis.sdiffstore("tar".getBytes(), bfoo, bbar, bcar);
    assertEquals(2, bstatus);
    assertByteArraySetEquals(bexpected, cuckooJedis.smembers(bcar));

  }

  @Test
  public void srandmember() {
    cuckooJedis.sadd("foo", "a");
    cuckooJedis.sadd("foo", "b");

    String member = cuckooJedis.srandmember("foo");

    assertTrue("a".equals(member) || "b".equals(member));
    assertEquals(2, cuckooJedis.smembers("foo").size());

    member = cuckooJedis.srandmember("bar");
    assertNull(member);

    // Binary
    cuckooJedis.sadd(bfoo, ba);
    cuckooJedis.sadd(bfoo, bb);

    byte[] bmember = cuckooJedis.srandmember(bfoo);

    assertTrue(Arrays.equals(ba, bmember) || Arrays.equals(bb, bmember));
    assertEquals(2, cuckooJedis.smembers(bfoo).size());

    bmember = cuckooJedis.srandmember(bbar);
    assertNull(bmember);
  }

  @Test
  public void sscan() {
    cuckooJedis.sadd("foo", "a", "b");

    ScanResult<String> result = cuckooJedis.sscan("foo", SCAN_POINTER_START);

    assertEquals(SCAN_POINTER_START, result.getCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    cuckooJedis.sadd(bfoo, ba, bb);

    ScanResult<byte[]> bResult = cuckooJedis.sscan(bfoo, SCAN_POINTER_START_BINARY);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void sscanMatch() {
    ScanParams params = new ScanParams();
    params.match("a*");

    cuckooJedis.sadd("foo", "b", "a", "aa");
    ScanResult<String> result = cuckooJedis.sscan("foo", SCAN_POINTER_START, params);

    assertEquals(SCAN_POINTER_START, result.getCursor());
    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.match(bbarstar);

    cuckooJedis.sadd(bfoo, bbar1, bbar2, bbar3);
    ScanResult<byte[]> bResult = cuckooJedis.sscan(bfoo, SCAN_POINTER_START_BINARY, params);

    assertArrayEquals(SCAN_POINTER_START_BINARY, bResult.getCursorAsBytes());
    assertFalse(bResult.getResult().isEmpty());
  }

  @Test
  public void sscanCount() {
    ScanParams params = new ScanParams();
    params.count(2);

    cuckooJedis.sadd("foo", "a1", "a2", "a3", "a4", "a5");

    ScanResult<String> result = cuckooJedis.sscan("foo", SCAN_POINTER_START, params);

    assertFalse(result.getResult().isEmpty());

    // binary
    params = new ScanParams();
    params.count(2);

    cuckooJedis.sadd(bfoo, bbar1, bbar2, bbar3);
    ScanResult<byte[]> bResult = cuckooJedis.sscan(bfoo, SCAN_POINTER_START_BINARY, params);

    assertFalse(bResult.getResult().isEmpty());
  }
}
