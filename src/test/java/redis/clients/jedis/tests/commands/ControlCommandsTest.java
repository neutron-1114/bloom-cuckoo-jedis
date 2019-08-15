package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import redis.clients.jedis.DebugParams;
import redis.clients.jedis.CuckooJedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.exceptions.JedisDataException;

public class ControlCommandsTest extends CuckooJedisCommandTestBase {
  @Test
  public void save() {
    try {
      String status = cuckooJedis.save();
      assertEquals("OK", status);
    } catch (JedisDataException e) {
      assertTrue("ERR Background save already in progress".equalsIgnoreCase(e.getMessage()));
    }
  }

  @Test
  public void bgsave() {
    try {
      String status = cuckooJedis.bgsave();
      assertEquals("Background saving started", status);
    } catch (JedisDataException e) {
      assertTrue("ERR Background save already in progress".equalsIgnoreCase(e.getMessage()));
    }
  }

  @Test
  public void bgrewriteaof() {
    String scheduled = "Background append only file rewriting scheduled";
    String started = "Background append only file rewriting started";

    String status = cuckooJedis.bgrewriteaof();

    boolean ok = status.equals(scheduled) || status.equals(started);
    assertTrue(ok);
  }

  @Test
  public void lastsave() throws InterruptedException {
    long saved = cuckooJedis.lastsave();
    assertTrue(saved > 0);
  }

  @Test
  public void info() {
    String info = cuckooJedis.info();
    assertNotNull(info);
    info = cuckooJedis.info("server");
    assertNotNull(info);
  }

  @Test
  public void readonly() {
    try {
      cuckooJedis.readonly();
    } catch (JedisDataException e) {
      assertTrue("ERR This instance has cluster support disabled".equalsIgnoreCase(e.getMessage()));
    }
  }

  @Test
  public void monitor() {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // sleep 100ms to make sure that monitor thread runs first
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
        CuckooJedis j = new CuckooJedis("localhost");
        j.auth("foobared");
        for (int i = 0; i < 5; i++) {
          j.incr("foobared");
        }
        j.disconnect();
      }
    }).start();

    cuckooJedis.monitor(new JedisMonitor() {
      private int count = 0;

      @Override
      public void onCommand(String command) {
        if (command.contains("INCR")) {
          count++;
        }
        if (count == 5) {
          client.disconnect();
        }
      }
    });
  }

  @Test
  public void configGet() {
    List<String> info = cuckooJedis.configGet("m*");
    assertNotNull(info);
  }

  @Test
  public void configSet() {
    List<String> info = cuckooJedis.configGet("maxmemory");
    String memory = info.get(1);
    String status = cuckooJedis.configSet("maxmemory", "200");
    assertEquals("OK", status);
    cuckooJedis.configSet("maxmemory", memory);
  }

  @Test
  public void sync() {
    cuckooJedis.sync();
  }

  @Test
  public void debug() {
    cuckooJedis.set("foo", "bar");
    String resp = cuckooJedis.debug(DebugParams.OBJECT("foo"));
    assertNotNull(resp);
    resp = cuckooJedis.debug(DebugParams.RELOAD());
    assertNotNull(resp);
  }

  @Test
  public void waitReplicas() {
    Long replicas = cuckooJedis.waitReplicas(1, 100);
    assertEquals(1, replicas.longValue());
  }

  @Test
  public void clientPause() throws InterruptedException, ExecutionException {
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    try {
      final CuckooJedis cuckooJedisToPause1 = createJedis();
      final CuckooJedis cuckooJedisToPause2 = createJedis();

      int pauseMillis = 1250;
      cuckooJedis.clientPause(pauseMillis);

      Future<Long> latency1 = executorService.submit(new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          long startMillis = System.currentTimeMillis();
          assertEquals("PONG", cuckooJedisToPause1.ping());
          return System.currentTimeMillis() - startMillis;
        }
      });
      Future<Long> latency2 = executorService.submit(new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          long startMillis = System.currentTimeMillis();
          assertEquals("PONG", cuckooJedisToPause2.ping());
          return System.currentTimeMillis() - startMillis;
        }
      });

      long latencyMillis1 = latency1.get();
      long latencyMillis2 = latency2.get();

      int pauseMillisDelta = 100;
      assertTrue(pauseMillis <= latencyMillis1 && latencyMillis1 <= pauseMillis + pauseMillisDelta);
      assertTrue(pauseMillis <= latencyMillis2 && latencyMillis2 <= pauseMillis + pauseMillisDelta);

      cuckooJedisToPause1.close();
      cuckooJedisToPause2.close();
    } finally {
      executorService.shutdown();
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    }
  }

  @Test
  public void memoryDoctorString() {
    String memoryInfo = cuckooJedis.memoryDoctor();
    assertNotNull(memoryInfo);
  }

  @Test
  public void memoryDoctorBinary() {
    byte[] memoryInfo = cuckooJedis.memoryDoctorBinary();
    assertNotNull(memoryInfo);
  }
}