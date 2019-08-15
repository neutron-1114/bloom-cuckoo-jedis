package redis.clients.jedis.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import redis.clients.jedis.Module;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.tests.commands.CuckooJedisCommandTestBase;
import redis.clients.jedis.util.SafeEncoder;

public class ModuleTest extends CuckooJedisCommandTestBase {

  static enum ModuleCommand implements ProtocolCommand {
    SIMPLE("testmodule.simple")  ;

    private final byte[] raw;

    ModuleCommand(String alt) {
      raw = SafeEncoder.encode(alt);
    }

    @Override
    public byte[] getRaw() {
      return raw;
    }
  }

  @Test
  public void testModules() {
    String res = cuckooJedis.moduleLoad("/tmp/testmodule.so");
    assertEquals("OK", res);

    List<Module> modules = cuckooJedis.moduleList();

    assertEquals("testmodule", modules.get(0).getName());

    cuckooJedis.getClient().sendCommand(ModuleCommand.SIMPLE);
    Long out = cuckooJedis.getClient().getIntegerReply();
    assertTrue(out > 0);

    res = cuckooJedis.moduleUnload("testmodule");
    assertEquals("OK", res);
  }

}