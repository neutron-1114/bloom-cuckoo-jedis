package redis.clients.jedis.tests.utils;

import redis.clients.jedis.CuckooJedis;
import redis.clients.jedis.HostAndPort;

public class ClientKillerUtil {
  public static void killClient(CuckooJedis cuckooJedis, String clientName) {
    for (String clientInfo : cuckooJedis.clientList().split("\n")) {
      if (clientInfo.contains("name=" + clientName)) {
        // Ugly, but cmon, it's a test.
        String hostAndPortString  = clientInfo.split(" ")[1].split("=")[1];
        String[] hostAndPortParts = HostAndPort.extractParts(hostAndPortString);
        // It would be better if we kill the client by Id as it's safer but cuckooJedis doesn't implement
        // the command yet.
        cuckooJedis.clientKill(hostAndPortParts[0] + ":" + hostAndPortParts[1]);
      }
    }
  }

  public static void tagClient(CuckooJedis j, String name) {
    j.clientSetname(name);
  }
}
