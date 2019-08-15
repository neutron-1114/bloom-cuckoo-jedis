package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import redis.clients.jedis.BitOP;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

import java.util.List;

public class BitCommandsTest extends CuckooJedisCommandTestBase {
  @Test
  public void setAndgetbit() {
    boolean bit = cuckooJedis.setbit("foo", 0, true);
    assertEquals(false, bit);

    bit = cuckooJedis.getbit("foo", 0);
    assertEquals(true, bit);

    boolean bbit = cuckooJedis.setbit("bfoo".getBytes(), 0, "1".getBytes());
    assertFalse(bbit);

    bbit = cuckooJedis.getbit("bfoo".getBytes(), 0);
    assertTrue(bbit);
  }

  @Test
  public void bitpos() {
    String foo = "foo";

    cuckooJedis.set(foo, String.valueOf(0));

    cuckooJedis.setbit(foo, 3, true);
    cuckooJedis.setbit(foo, 7, true);
    cuckooJedis.setbit(foo, 13, true);
    cuckooJedis.setbit(foo, 39, true);

    /*
     * byte: 0 1 2 3 4 bit: 00010001 / 00000100 / 00000000 / 00000000 / 00000001
     */
    long offset = cuckooJedis.bitpos(foo, true);
    assertEquals(2, offset);
    offset = cuckooJedis.bitpos(foo, false);
    assertEquals(0, offset);

    offset = cuckooJedis.bitpos(foo, true, new BitPosParams(1));
    assertEquals(13, offset);
    offset = cuckooJedis.bitpos(foo, false, new BitPosParams(1));
    assertEquals(8, offset);

    offset = cuckooJedis.bitpos(foo, true, new BitPosParams(2, 3));
    assertEquals(-1, offset);
    offset = cuckooJedis.bitpos(foo, false, new BitPosParams(2, 3));
    assertEquals(16, offset);

    offset = cuckooJedis.bitpos(foo, true, new BitPosParams(3, 4));
    assertEquals(39, offset);
  }

  @Test
  public void bitposBinary() {
    // binary
    byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };

    cuckooJedis.set(bfoo, Protocol.toByteArray(0));

    cuckooJedis.setbit(bfoo, 3, true);
    cuckooJedis.setbit(bfoo, 7, true);
    cuckooJedis.setbit(bfoo, 13, true);
    cuckooJedis.setbit(bfoo, 39, true);

    /*
     * byte: 0 1 2 3 4 bit: 00010001 / 00000100 / 00000000 / 00000000 / 00000001
     */
    long offset = cuckooJedis.bitpos(bfoo, true);
    assertEquals(2, offset);
    offset = cuckooJedis.bitpos(bfoo, false);
    assertEquals(0, offset);

    offset = cuckooJedis.bitpos(bfoo, true, new BitPosParams(1));
    assertEquals(13, offset);
    offset = cuckooJedis.bitpos(bfoo, false, new BitPosParams(1));
    assertEquals(8, offset);

    offset = cuckooJedis.bitpos(bfoo, true, new BitPosParams(2, 3));
    assertEquals(-1, offset);
    offset = cuckooJedis.bitpos(bfoo, false, new BitPosParams(2, 3));
    assertEquals(16, offset);

    offset = cuckooJedis.bitpos(bfoo, true, new BitPosParams(3, 4));
    assertEquals(39, offset);
  }

  @Test
  public void bitposWithNoMatchingBitExist() {
    String foo = "foo";

    cuckooJedis.set(foo, String.valueOf(0));
    for (int idx = 0; idx < 8; idx++) {
      cuckooJedis.setbit(foo, idx, true);
    }

    /*
     * byte: 0 bit: 11111111
     */
    long offset = cuckooJedis.bitpos(foo, false);
    // offset should be last index + 1
    assertEquals(8, offset);
  }

  @Test
  public void bitposWithNoMatchingBitExistWithinRange() {
    String foo = "foo";

    cuckooJedis.set(foo, String.valueOf(0));
    for (int idx = 0; idx < 8 * 5; idx++) {
      cuckooJedis.setbit(foo, idx, true);
    }

    /*
     * byte: 0 1 2 3 4 bit: 11111111 / 11111111 / 11111111 / 11111111 / 11111111
     */
    long offset = cuckooJedis.bitpos(foo, false, new BitPosParams(2, 3));
    // offset should be -1
    assertEquals(-1, offset);
  }

  @Test
  public void setAndgetrange() {
    cuckooJedis.set("key1", "Hello World");
    long reply = cuckooJedis.setrange("key1", 6, "CuckooJedis");
    assertEquals(11, reply);

    assertEquals("Hello CuckooJedis", cuckooJedis.get("key1"));

    assertEquals("Hello", cuckooJedis.getrange("key1", 0, 4));
    assertEquals("CuckooJedis", cuckooJedis.getrange("key1", 6, 11));
  }

  @Test
  public void bitCount() {
    cuckooJedis.setbit("foo", 16, true);
    cuckooJedis.setbit("foo", 24, true);
    cuckooJedis.setbit("foo", 40, true);
    cuckooJedis.setbit("foo", 56, true);

    long c4 = cuckooJedis.bitcount("foo");
    assertEquals(4, c4);

    long c3 = cuckooJedis.bitcount("foo", 2L, 5L);
    assertEquals(3, c3);
  }

  @Test
  public void bitOp() {
    cuckooJedis.set("key1", "\u0060");
    cuckooJedis.set("key2", "\u0044");

    cuckooJedis.bitop(BitOP.AND, "resultAnd", "key1", "key2");
    String resultAnd = cuckooJedis.get("resultAnd");
    assertEquals("\u0040", resultAnd);

    cuckooJedis.bitop(BitOP.OR, "resultOr", "key1", "key2");
    String resultOr = cuckooJedis.get("resultOr");
    assertEquals("\u0064", resultOr);

    cuckooJedis.bitop(BitOP.XOR, "resultXor", "key1", "key2");
    String resultXor = cuckooJedis.get("resultXor");
    assertEquals("\u0024", resultXor);
  }

  @Test
  public void bitOpNot() {
    cuckooJedis.setbit("key", 0, true);
    cuckooJedis.setbit("key", 4, true);

    cuckooJedis.bitop(BitOP.NOT, "resultNot", "key");

    String resultNot = cuckooJedis.get("resultNot");
    assertEquals("\u0077", resultNot);
  }

  @Test(expected = redis.clients.jedis.exceptions.JedisDataException.class)
  public void bitOpNotMultiSourceShouldFail() {
    cuckooJedis.bitop(BitOP.NOT, "dest", "src1", "src2");
  }

  @Test
  public void testBitfield() {
    List<Long> responses = cuckooJedis.bitfield("mykey", "INCRBY","i5","100","1", "GET", "u4", "0");
    assertEquals(1L, responses.get(0).longValue());
    assertEquals(0L, responses.get(1).longValue());
  }

  @Test
  public void testBinaryBitfield() {
    List<Long> responses = cuckooJedis.bitfield(SafeEncoder.encode("mykey"), SafeEncoder.encode("INCRBY"),
            SafeEncoder.encode("i5"), SafeEncoder.encode("100"), SafeEncoder.encode("1"),
            SafeEncoder.encode("GET"), SafeEncoder.encode("u4"), SafeEncoder.encode("0")
    );
    assertEquals(1L, responses.get(0).longValue());
    assertEquals(0L, responses.get(1).longValue());
  }

}
