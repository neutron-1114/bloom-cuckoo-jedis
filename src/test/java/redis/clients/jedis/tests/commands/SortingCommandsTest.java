package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;
import static redis.clients.jedis.tests.utils.AssertUtil.assertByteArrayListEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import redis.clients.jedis.SortingParams;

public class SortingCommandsTest extends CuckooJedisCommandTestBase {
  final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
  final byte[] bbar1 = { 0x05, 0x06, 0x07, 0x08, '1' };
  final byte[] bbar2 = { 0x05, 0x06, 0x07, 0x08, '2' };
  final byte[] bbar3 = { 0x05, 0x06, 0x07, 0x08, '3' };
  final byte[] bbar10 = { 0x05, 0x06, 0x07, 0x08, '1', '0' };
  final byte[] bbarstar = { 0x05, 0x06, 0x07, 0x08, '*' };
  final byte[] bcar1 = { 0x0A, 0x0B, 0x0C, 0x0D, '1' };
  final byte[] bcar2 = { 0x0A, 0x0B, 0x0C, 0x0D, '2' };
  final byte[] bcar10 = { 0x0A, 0x0B, 0x0C, 0x0D, '1', '0' };
  final byte[] bcarstar = { 0x0A, 0x0B, 0x0C, 0x0D, '*' };
  final byte[] b1 = { '1' };
  final byte[] b2 = { '2' };
  final byte[] b3 = { '3' };
  final byte[] b10 = { '1', '0' };

  @Test
  public void sort() {
    cuckooJedis.lpush("foo", "3");
    cuckooJedis.lpush("foo", "2");
    cuckooJedis.lpush("foo", "1");

    List<String> result = cuckooJedis.sort("foo");

    List<String> expected = new ArrayList<String>();
    expected.add("1");
    expected.add("2");
    expected.add("3");

    assertEquals(expected, result);

    // Binary
    cuckooJedis.lpush(bfoo, b3);
    cuckooJedis.lpush(bfoo, b2);
    cuckooJedis.lpush(bfoo, b1);

    List<byte[]> bresult = cuckooJedis.sort(bfoo);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b1);
    bexpected.add(b2);
    bexpected.add(b3);

    assertByteArrayListEquals(bexpected, bresult);
  }

  @Test
  public void sortBy() {
    cuckooJedis.lpush("foo", "2");
    cuckooJedis.lpush("foo", "3");
    cuckooJedis.lpush("foo", "1");

    cuckooJedis.set("bar1", "3");
    cuckooJedis.set("bar2", "2");
    cuckooJedis.set("bar3", "1");

    SortingParams sp = new SortingParams();
    sp.by("bar*");

    List<String> result = cuckooJedis.sort("foo", sp);

    List<String> expected = new ArrayList<String>();
    expected.add("3");
    expected.add("2");
    expected.add("1");

    assertEquals(expected, result);

    // Binary
    cuckooJedis.lpush(bfoo, b2);
    cuckooJedis.lpush(bfoo, b3);
    cuckooJedis.lpush(bfoo, b1);

    cuckooJedis.set(bbar1, b3);
    cuckooJedis.set(bbar2, b2);
    cuckooJedis.set(bbar3, b1);

    SortingParams bsp = new SortingParams();
    bsp.by(bbarstar);

    List<byte[]> bresult = cuckooJedis.sort(bfoo, bsp);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b3);
    bexpected.add(b2);
    bexpected.add(b1);

    assertByteArrayListEquals(bexpected, bresult);

  }

  @Test
  public void sortDesc() {
    cuckooJedis.lpush("foo", "3");
    cuckooJedis.lpush("foo", "2");
    cuckooJedis.lpush("foo", "1");

    SortingParams sp = new SortingParams();
    sp.desc();

    List<String> result = cuckooJedis.sort("foo", sp);

    List<String> expected = new ArrayList<String>();
    expected.add("3");
    expected.add("2");
    expected.add("1");

    assertEquals(expected, result);

    // Binary
    cuckooJedis.lpush(bfoo, b3);
    cuckooJedis.lpush(bfoo, b2);
    cuckooJedis.lpush(bfoo, b1);

    SortingParams bsp = new SortingParams();
    bsp.desc();

    List<byte[]> bresult = cuckooJedis.sort(bfoo, bsp);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b3);
    bexpected.add(b2);
    bexpected.add(b1);

    assertByteArrayListEquals(bexpected, bresult);
  }

  @Test
  public void sortLimit() {
    for (int n = 10; n > 0; n--) {
      cuckooJedis.lpush("foo", String.valueOf(n));
    }

    SortingParams sp = new SortingParams();
    sp.limit(0, 3);

    List<String> result = cuckooJedis.sort("foo", sp);

    List<String> expected = new ArrayList<String>();
    expected.add("1");
    expected.add("2");
    expected.add("3");

    assertEquals(expected, result);

    // Binary
    cuckooJedis.rpush(bfoo, new byte[] { (byte) '4' });
    cuckooJedis.rpush(bfoo, new byte[] { (byte) '3' });
    cuckooJedis.rpush(bfoo, new byte[] { (byte) '2' });
    cuckooJedis.rpush(bfoo, new byte[] { (byte) '1' });

    SortingParams bsp = new SortingParams();
    bsp.limit(0, 3);

    List<byte[]> bresult = cuckooJedis.sort(bfoo, bsp);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b1);
    bexpected.add(b2);
    bexpected.add(b3);

    assertByteArrayListEquals(bexpected, bresult);
  }

  @Test
  public void sortAlpha() {
    cuckooJedis.lpush("foo", "1");
    cuckooJedis.lpush("foo", "2");
    cuckooJedis.lpush("foo", "10");

    SortingParams sp = new SortingParams();
    sp.alpha();

    List<String> result = cuckooJedis.sort("foo", sp);

    List<String> expected = new ArrayList<String>();
    expected.add("1");
    expected.add("10");
    expected.add("2");

    assertEquals(expected, result);

    // Binary
    cuckooJedis.lpush(bfoo, b1);
    cuckooJedis.lpush(bfoo, b2);
    cuckooJedis.lpush(bfoo, b10);

    SortingParams bsp = new SortingParams();
    bsp.alpha();

    List<byte[]> bresult = cuckooJedis.sort(bfoo, bsp);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b1);
    bexpected.add(b10);
    bexpected.add(b2);

    assertByteArrayListEquals(bexpected, bresult);
  }

  @Test
  public void sortGet() {
    cuckooJedis.lpush("foo", "1");
    cuckooJedis.lpush("foo", "2");
    cuckooJedis.lpush("foo", "10");

    cuckooJedis.set("bar1", "bar1");
    cuckooJedis.set("bar2", "bar2");
    cuckooJedis.set("bar10", "bar10");

    cuckooJedis.set("car1", "car1");
    cuckooJedis.set("car2", "car2");
    cuckooJedis.set("car10", "car10");

    SortingParams sp = new SortingParams();
    sp.get("car*", "bar*");

    List<String> result = cuckooJedis.sort("foo", sp);

    List<String> expected = new ArrayList<String>();
    expected.add("car1");
    expected.add("bar1");
    expected.add("car2");
    expected.add("bar2");
    expected.add("car10");
    expected.add("bar10");

    assertEquals(expected, result);

    // Binary
    cuckooJedis.lpush(bfoo, b1);
    cuckooJedis.lpush(bfoo, b2);
    cuckooJedis.lpush(bfoo, b10);

    cuckooJedis.set(bbar1, bbar1);
    cuckooJedis.set(bbar2, bbar2);
    cuckooJedis.set(bbar10, bbar10);

    cuckooJedis.set(bcar1, bcar1);
    cuckooJedis.set(bcar2, bcar2);
    cuckooJedis.set(bcar10, bcar10);

    SortingParams bsp = new SortingParams();
    bsp.get(bcarstar, bbarstar);

    List<byte[]> bresult = cuckooJedis.sort(bfoo, bsp);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(bcar1);
    bexpected.add(bbar1);
    bexpected.add(bcar2);
    bexpected.add(bbar2);
    bexpected.add(bcar10);
    bexpected.add(bbar10);

    assertByteArrayListEquals(bexpected, bresult);
  }

  @Test
  public void sortStore() {
    cuckooJedis.lpush("foo", "1");
    cuckooJedis.lpush("foo", "2");
    cuckooJedis.lpush("foo", "10");

    long result = cuckooJedis.sort("foo", "result");

    List<String> expected = new ArrayList<String>();
    expected.add("1");
    expected.add("2");
    expected.add("10");

    assertEquals(3, result);
    assertEquals(expected, cuckooJedis.lrange("result", 0, 1000));

    // Binary
    cuckooJedis.lpush(bfoo, b1);
    cuckooJedis.lpush(bfoo, b2);
    cuckooJedis.lpush(bfoo, b10);

    byte[] bkresult = new byte[] { 0X09, 0x0A, 0x0B, 0x0C };
    long bresult = cuckooJedis.sort(bfoo, bkresult);

    List<byte[]> bexpected = new ArrayList<byte[]>();
    bexpected.add(b1);
    bexpected.add(b2);
    bexpected.add(b10);

    assertEquals(3, bresult);
    assertByteArrayListEquals(bexpected, cuckooJedis.lrange(bkresult, 0, 1000));
  }

}