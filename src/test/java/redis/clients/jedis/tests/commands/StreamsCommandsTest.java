package redis.clients.jedis.tests.commands;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;
import redis.clients.jedis.*;
import redis.clients.jedis.Protocol.Keyword;
import redis.clients.jedis.exceptions.JedisDataException;

public class StreamsCommandsTest extends CuckooJedisCommandTestBase {

  @Test
  public void xadd() {
    
    try {
      Map<String,String> map1 = new HashMap<String, String>();
      cuckooJedis.xadd("stream1", null, map1);
      fail();
    } catch (JedisDataException expected) {
      assertEquals("ERR wrong number of arguments for 'xadd' command", expected.getMessage());
    }
    
    Map<String,String> map1 = new HashMap<String, String>();
    map1.put("f1", "v1");
    StreamEntryID id1 = cuckooJedis.xadd("xadd-stream1", null, map1);
    assertNotNull(id1);	

    Map<String,String> map2 = new HashMap<String, String>();
    map2.put("f1", "v1");
    map2.put("f2", "v2");
    StreamEntryID id2 = cuckooJedis.xadd("xadd-stream1", null, map2);
    assertTrue(id2.compareTo(id1) > 0);

    Map<String,String> map3 = new HashMap<String, String>();
    map3.put("f2", "v2");
    map3.put("f3", "v3");
    StreamEntryID id3 = cuckooJedis.xadd("xadd-stream2", null, map3);

    Map<String,String> map4 = new HashMap<String, String>();
    map4.put("f2", "v2");
    map4.put("f3", "v3");
    StreamEntryID idIn = new StreamEntryID(id3.getTime()+1, 1L);
    StreamEntryID id4 = cuckooJedis.xadd("xadd-stream2", idIn, map4);
    assertEquals(idIn, id4);
    assertTrue(id4.compareTo(id3) > 0);
    
    Map<String,String> map5 = new HashMap<String, String>();
    map3.put("f4", "v4");
    map3.put("f5", "v5");
    StreamEntryID id5 = cuckooJedis.xadd("xadd-stream2", null, map3);
    assertTrue(id5.compareTo(id4) > 0);    
    
    Map<String,String> map6 = new HashMap<String, String>();
    map3.put("f4", "v4");
    map3.put("f5", "v5");
    StreamEntryID id6 = cuckooJedis.xadd("xadd-stream2", null, map3, 3, false);
    assertTrue(id6.compareTo(id5) > 0);
    assertEquals(3L, cuckooJedis.xlen("xadd-stream2").longValue());
  }
  
  @Test
  public void xdel() {
    Map<String,String> map1 = new HashMap<String, String>();
    map1.put("f1", "v1");
    
    StreamEntryID id1 = cuckooJedis.xadd("xdel-stream", null, map1);
    assertNotNull(id1); 
    
    StreamEntryID id2 = cuckooJedis.xadd("xdel-stream", null, map1);
    assertNotNull(id2);
    assertEquals(2L, cuckooJedis.xlen("xdel-stream").longValue());


    assertEquals(1L, cuckooJedis.xdel("xdel-stream", id1));
    assertEquals(1L, cuckooJedis.xlen("xdel-stream").longValue());
  }

  @Test
  public void xlen() {
    assertEquals(0L, cuckooJedis.xlen("xlen-stream").longValue());
    
    Map<String,String> map = new HashMap<String, String>();
    map.put("f1", "v1");
    cuckooJedis.xadd("xlen-stream", null, map);
    assertEquals(1L, cuckooJedis.xlen("xlen-stream").longValue());
    
    cuckooJedis.xadd("xlen-stream", null, map);
    assertEquals(2L, cuckooJedis.xlen("xlen-stream").longValue());
  }

  @Test
  public void xrange() {
    List<StreamEntry> range = cuckooJedis.xrange("xrange-stream", (StreamEntryID)null, (StreamEntryID)null, Integer.MAX_VALUE);
    assertEquals(0, range.size());
        
    Map<String,String> map = new HashMap<String, String>();
    map.put("f1", "v1");
    StreamEntryID id1 = cuckooJedis.xadd("xrange-stream", null, map);
    StreamEntryID id2 = cuckooJedis.xadd("xrange-stream", null, map);
    List<StreamEntry> range2 = cuckooJedis.xrange("xrange-stream", (StreamEntryID)null, (StreamEntryID)null, 3);
    assertEquals(2, range2.size());
    
    List<StreamEntry> range3 = cuckooJedis.xrange("xrange-stream", id1, null, 2);
    assertEquals(2, range3.size());
    
    List<StreamEntry> range4 = cuckooJedis.xrange("xrange-stream", id1, id2, 2);
    assertEquals(2, range4.size());

    List<StreamEntry> range5 = cuckooJedis.xrange("xrange-stream", id1, id2, 1);
    assertEquals(1, range5.size());
    
    List<StreamEntry> range6 = cuckooJedis.xrange("xrange-stream", id2, null, 4);
    assertEquals(1, range6.size());
    
    StreamEntryID id3 = cuckooJedis.xadd("xrange-stream", null, map);
    List<StreamEntry> range7 = cuckooJedis.xrange("xrange-stream", id2, id2, 4);
    assertEquals(1, range7.size());
  }
  
  @Test
  public void xread() {
    
    Entry<String, StreamEntryID> streamQeury1 = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("xread-stream1", new StreamEntryID());

    // Empty Stream
    List<Entry<String, List<StreamEntry>>> range = cuckooJedis.xread(1, 1L, streamQeury1);
    assertEquals(0, range.size());
    
    Map<String,String> map = new HashMap<String, String>();
    map.put("f1", "v1");
    StreamEntryID id1 = cuckooJedis.xadd("xread-stream1", null, map);
    StreamEntryID id2 = cuckooJedis.xadd("xread-stream2", null, map);
    
    // Read only a single Stream
    List<Entry<String, List<StreamEntry>>> streams1 = cuckooJedis.xread(1, 1L, streamQeury1);
    assertEquals(1, streams1.size());

    // Read from two Streams
    Entry<String, StreamEntryID> streamQuery2 = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("xread-stream1", new StreamEntryID());
    Entry<String, StreamEntryID> streamQuery3 = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("xread-stream2", new StreamEntryID());
    List<Entry<String, List<StreamEntry>>> streams2 = cuckooJedis.xread(2, 1L, streamQuery2, streamQuery3);
    assertEquals(2, streams2.size());

  }
  
  @Test
  public void xtrim() {
    Map<String,String> map1 = new HashMap<String, String>();
    map1.put("f1", "v1");
    
    cuckooJedis.xadd("xtrim-stream", null, map1);
    cuckooJedis.xadd("xtrim-stream", null, map1);
    cuckooJedis.xadd("xtrim-stream", null, map1);
    cuckooJedis.xadd("xtrim-stream", null, map1);
    cuckooJedis.xadd("xtrim-stream", null, map1);
    assertEquals(5L, cuckooJedis.xlen("xtrim-stream").longValue());

    cuckooJedis.xtrim("xtrim-stream", 3, false);
    assertEquals(3L, cuckooJedis.xlen("xtrim-stream").longValue());
  }
  
  @Test
  public void xrevrange() {
    List<StreamEntry> range = cuckooJedis.xrevrange("xrevrange-stream", (StreamEntryID)null, (StreamEntryID)null, Integer.MAX_VALUE);
    assertEquals(0, range.size());
        
    Map<String,String> map = new HashMap<String, String>();
    map.put("f1", "v1");
    StreamEntryID id1 = cuckooJedis.xadd("xrevrange-stream", null, map);
    StreamEntryID id2 = cuckooJedis.xadd("xrevrange-stream", null, map);
    List<StreamEntry> range2 = cuckooJedis.xrange("xrevrange-stream", (StreamEntryID)null, (StreamEntryID)null, 3);
    assertEquals(2, range2.size());
    
    List<StreamEntry> range3 = cuckooJedis.xrevrange("xrevrange-stream", null, id1, 2);
    assertEquals(2, range3.size());
    
    List<StreamEntry> range4 = cuckooJedis.xrevrange("xrevrange-stream", id2, id1, 2);
    assertEquals(2, range4.size());

    List<StreamEntry> range5 = cuckooJedis.xrevrange("xrevrange-stream", id2, id1, 1);
    assertEquals(1, range5.size());
    
    List<StreamEntry> range6 = cuckooJedis.xrevrange("xrevrange-stream", null, id2, 4);
    assertEquals(1, range6.size());
    
    StreamEntryID id3 = cuckooJedis.xadd("xrevrange-stream", null, map);
    List<StreamEntry> range7 = cuckooJedis.xrevrange("xrevrange-stream", id2, id2, 4);
    assertEquals(1, range7.size());

  }
  
  @Test
  public void xgroup() {
    
    Map<String,String> map = new HashMap<String, String>();
    map.put("f1", "v1");
    StreamEntryID id1 = cuckooJedis.xadd("xgroup-stream", null, map);
    
    String status = cuckooJedis.xgroupCreate("xgroup-stream", "consumer-group-name", null, false);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));


    status = cuckooJedis.xgroupSetID("xgroup-stream", "consumer-group-name", id1);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));

    status = cuckooJedis.xgroupCreate("xgroup-stream", "consumer-group-name1", StreamEntryID.LAST_ENTRY, false);
    assertTrue(Keyword.OK.name().equalsIgnoreCase(status));
    
    cuckooJedis.xgroupDestroy("xgroup-stream", "consumer-group-name");

    //TODO test xgroupDelConsumer
  }
  
  @Test
  public void xreadGroup() {
    
    // Simple xreadGroup with NOACK
    Map<String,String> map = new HashMap<>();
    map.put("f1", "v1");
    StreamEntryID id1 = cuckooJedis.xadd("xreadGroup-stream1", null, map);
    String status1 = cuckooJedis.xgroupCreate("xreadGroup-stream1", "xreadGroup-group", null, false);
    Entry<String, StreamEntryID> streamQeury1 = new AbstractMap.SimpleImmutableEntry<>("xreadGroup-stream1", StreamEntryID.UNRECEIVED_ENTRY);
    List<Entry<String, List<StreamEntry>>> range = cuckooJedis.xreadGroup("xreadGroup-group", "xreadGroup-consumer", 1, 0, true, streamQeury1);
    assertEquals(1, range.size());
    assertEquals(1, range.get(0).getValue().size());
    
    StreamEntryID id2 = cuckooJedis.xadd("xreadGroup-stream1", null, map);
    StreamEntryID id3 = cuckooJedis.xadd("xreadGroup-stream2", null, map);
    String status2 = cuckooJedis.xgroupCreate("xreadGroup-stream2", "xreadGroup-group", null, false);
   
    // Read only a single Stream
    Entry<String, StreamEntryID> streamQeury11 = new AbstractMap.SimpleImmutableEntry<>("xreadGroup-stream1", StreamEntryID.UNRECEIVED_ENTRY);
    List<Entry<String, List<StreamEntry>>> streams1 = cuckooJedis.xreadGroup("xreadGroup-group", "xreadGroup-consumer", 1, 1L, true, streamQeury11);
    assertEquals(1, streams1.size());
    assertEquals(1, streams1.get(0).getValue().size());

    // Read from two Streams
    Entry<String, StreamEntryID> streamQuery2 = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("xreadGroup-stream1", new StreamEntryID());
    Entry<String, StreamEntryID> streamQuery3 = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("xreadGroup-stream2", new StreamEntryID());
    List<Entry<String, List<StreamEntry>>> streams2 = cuckooJedis.xreadGroup("xreadGroup-group", "xreadGroup-consumer", 1, 1L, true, streamQuery2, streamQuery3);
    assertEquals(2, streams2.size());

    // Read only fresh messages
    StreamEntryID id4 = cuckooJedis.xadd("xreadGroup-stream1", null, map);
    Entry<String, StreamEntryID> streamQeuryFresh = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("xreadGroup-stream1", StreamEntryID.UNRECEIVED_ENTRY);
    List<Entry<String, List<StreamEntry>>> streams3 = cuckooJedis.xreadGroup("xreadGroup-group", "xreadGroup-consumer", 4, 100L, true, streamQeuryFresh);
    assertEquals(1, streams3.size());  
    assertEquals(id4, streams3.get(0).getValue().get(0).getID());
  }

  
  
  @Test
  public void xack() {
       
    Map<String,String> map = new HashMap<String, String>();
    map.put("f1", "v1");
    StreamEntryID id1 = cuckooJedis.xadd("xack-stream", null, map);
    
    String status = cuckooJedis.xgroupCreate("xack-stream", "xack-group", null, false);
    
    Entry<String, StreamEntryID> streamQeury1 = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("xack-stream", StreamEntryID.UNRECEIVED_ENTRY);

    // Empty Stream
    List<Entry<String, List<StreamEntry>>> range = cuckooJedis.xreadGroup("xack-group", "xack-consumer", 1, 1L, false, streamQeury1);
    assertEquals(1, range.size());

    assertEquals(1L, cuckooJedis.xack("xack-stream", "xack-group", range.get(0).getValue().get(0).getID()));
  }
  
  @Test
  public void xpendeing() {       
    Map<String,String> map = new HashMap<String, String>();
    map.put("f1", "v1");
    StreamEntryID id1 = cuckooJedis.xadd("xpendeing-stream", null, map);
    
    String status = cuckooJedis.xgroupCreate("xpendeing-stream", "xpendeing-group", null, false);
    
    Entry<String, StreamEntryID> streamQeury1 = new AbstractMap.SimpleImmutableEntry<String, StreamEntryID>("xpendeing-stream", StreamEntryID.UNRECEIVED_ENTRY);

    // Empty Stream
    List<Entry<String, List<StreamEntry>>> range = cuckooJedis.xreadGroup("xpendeing-group", "xpendeing-consumer", 1, 1L, false, streamQeury1);
    assertEquals(1, range.size());
    assertEquals(1, range.get(0).getValue().size());
    
    List<StreamPendingEntry> pendingRange = cuckooJedis.xpending("xpendeing-stream", "xpendeing-group", null, null, 3, "xpendeing-consumer");
    assertEquals(1, pendingRange.size());
    
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    
    cuckooJedis.xclaim("xpendeing-stream", "xpendeing-group", "xpendeing-consumer2", 500, 0, 0, false, pendingRange.get(0).getID());
  }

  @Test
  public void pipeline() {
    Map<String,String> map = new HashMap<>();
    map.put("a", "b");
    Pipeline p = cuckooJedis.pipelined();
    Response<StreamEntryID> id1 = p.xadd("stream1", StreamEntryID.NEW_ENTRY, map);
    Response<StreamEntryID> id2 = p.xadd("stream1", StreamEntryID.NEW_ENTRY, map);
    Response<List<StreamEntry>> results = p.xrange("stream1", null, null, 2);
    p.sync();

    List<StreamEntry> entries = results.get();
    assertEquals(2, entries.size());
    assertEquals(id1.get(), entries.get(0).getID());
    assertEquals(map, entries.get(0).getFields());
    assertEquals(id2.get(), entries.get(1).getID());
    assertEquals(map, entries.get(1).getFields());
  }

  @Test
  public void transaction() {
    Map<String,String> map = new HashMap<>();
    map.put("a", "b");
    Transaction t = cuckooJedis.multi();
    Response<StreamEntryID> id1 = t.xadd("stream1", StreamEntryID.NEW_ENTRY, map);
    Response<StreamEntryID> id2 = t.xadd("stream1", StreamEntryID.NEW_ENTRY, map);
    Response<List<StreamEntry>> results = t.xrange("stream1", null, null, 2);
    t.exec();

    List<StreamEntry> entries = results.get();
    assertEquals(2, entries.size());
    assertEquals(id1.get(), entries.get(0).getID());
    assertEquals(map, entries.get(0).getFields());
    assertEquals(id2.get(), entries.get(1).getID());
    assertEquals(map, entries.get(1).getFields());
  }
}
