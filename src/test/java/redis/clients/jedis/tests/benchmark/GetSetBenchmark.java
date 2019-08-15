package redis.clients.jedis.tests.benchmark;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Calendar;

import redis.clients.jedis.CuckooJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.tests.HostAndPortUtil;

public class GetSetBenchmark {
  private static HostAndPort hnp = HostAndPortUtil.getRedisServers().get(0);
  private static final int TOTAL_OPERATIONS = 100000;

  public static void main(String[] args) throws UnknownHostException, IOException {
    CuckooJedis cuckooJedis = new CuckooJedis(hnp);
    cuckooJedis.connect();
    cuckooJedis.auth("foobared");
    cuckooJedis.flushAll();

    long begin = Calendar.getInstance().getTimeInMillis();

    for (int n = 0; n <= TOTAL_OPERATIONS; n++) {
      String key = "foo" + n;
      cuckooJedis.set(key, "bar" + n);
      cuckooJedis.get(key);
    }

    long elapsed = Calendar.getInstance().getTimeInMillis() - begin;

    cuckooJedis.disconnect();

    System.out.println(((1000 * 2 * TOTAL_OPERATIONS) / elapsed) + " ops");
  }
}