package redis.clients.jedis.tests.commands;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.util.SafeEncoder;

public class GeoCommandsTest extends CuckooJedisCommandTestBase {
  final byte[] bfoo = { 0x01, 0x02, 0x03, 0x04 };
  final byte[] bA = { 0x0A };
  final byte[] bB = { 0x0B };
  final byte[] bC = { 0x0C };
  final byte[] bD = { 0x0D };
  final byte[] bNotexist = { 0x0F };

  @Test
  public void geoadd() {
    long size = cuckooJedis.geoadd("foo", 1, 2, "a");
    assertEquals(1, size);
    size = cuckooJedis.geoadd("foo", 2, 3, "a");
    assertEquals(0, size);

    Map<String, GeoCoordinate> coordinateMap = new HashMap<String, GeoCoordinate>();
    coordinateMap.put("a", new GeoCoordinate(3, 4));
    coordinateMap.put("b", new GeoCoordinate(2, 3));
    coordinateMap.put("c", new GeoCoordinate(3.314, 2.3241));

    size = cuckooJedis.geoadd("foo", coordinateMap);
    assertEquals(2, size);

    // binary
    size = cuckooJedis.geoadd(bfoo, 1, 2, bA);
    assertEquals(1, size);
    size = cuckooJedis.geoadd(bfoo, 2, 3, bA);
    assertEquals(0, size);

    Map<byte[], GeoCoordinate> bcoordinateMap = new HashMap<byte[], GeoCoordinate>();
    bcoordinateMap.put(bA, new GeoCoordinate(3, 4));
    bcoordinateMap.put(bB, new GeoCoordinate(2, 3));
    bcoordinateMap.put(bC, new GeoCoordinate(3.314, 2.3241));

    size = cuckooJedis.geoadd(bfoo, bcoordinateMap);
    assertEquals(2, size);
  }

  @Test
  public void geodist() {
    prepareGeoData();

    Double dist = cuckooJedis.geodist("foo", "a", "b");
    assertEquals(157149, dist.intValue());

    dist = cuckooJedis.geodist("foo", "a", "b", GeoUnit.KM);
    assertEquals(157, dist.intValue());

    dist = cuckooJedis.geodist("foo", "a", "b", GeoUnit.MI);
    assertEquals(97, dist.intValue());

    dist = cuckooJedis.geodist("foo", "a", "b", GeoUnit.FT);
    assertEquals(515583, dist.intValue());

    // binary
    dist = cuckooJedis.geodist(bfoo, bA, bB);
    assertEquals(157149, dist.intValue());

    dist = cuckooJedis.geodist(bfoo, bA, bB, GeoUnit.KM);
    assertEquals(157, dist.intValue());

    dist = cuckooJedis.geodist(bfoo, bA, bB, GeoUnit.MI);
    assertEquals(97, dist.intValue());

    dist = cuckooJedis.geodist(bfoo, bA, bB, GeoUnit.FT);
    assertEquals(515583, dist.intValue());
  }

  @Test
  public void geohash() {
    prepareGeoData();

    List<String> hashes = cuckooJedis.geohash("foo", "a", "b", "notexist");
    assertEquals(3, hashes.size());
    assertEquals("s0dnu20t9j0", hashes.get(0));
    assertEquals("s093jd0k720", hashes.get(1));
    assertNull(hashes.get(2));

    // binary
    List<byte[]> bhashes = cuckooJedis.geohash(bfoo, bA, bB, bNotexist);
    assertEquals(3, bhashes.size());
    assertArrayEquals(SafeEncoder.encode("s0dnu20t9j0"), bhashes.get(0));
    assertArrayEquals(SafeEncoder.encode("s093jd0k720"), bhashes.get(1));
    assertNull(bhashes.get(2));
  }

  @Test
  public void geopos() {
    prepareGeoData();

    List<GeoCoordinate> coordinates = cuckooJedis.geopos("foo", "a", "b", "notexist");
    assertEquals(3, coordinates.size());
    assertTrue(equalsWithinEpsilon(3.0, coordinates.get(0).getLongitude()));
    assertTrue(equalsWithinEpsilon(4.0, coordinates.get(0).getLatitude()));
    assertTrue(equalsWithinEpsilon(2.0, coordinates.get(1).getLongitude()));
    assertTrue(equalsWithinEpsilon(3.0, coordinates.get(1).getLatitude()));
    assertNull(coordinates.get(2));

    List<GeoCoordinate> bcoordinates = cuckooJedis.geopos(bfoo, bA, bB, bNotexist);
    assertEquals(3, bcoordinates.size());
    assertTrue(equalsWithinEpsilon(3.0, bcoordinates.get(0).getLongitude()));
    assertTrue(equalsWithinEpsilon(4.0, bcoordinates.get(0).getLatitude()));
    assertTrue(equalsWithinEpsilon(2.0, bcoordinates.get(1).getLongitude()));
    assertTrue(equalsWithinEpsilon(3.0, bcoordinates.get(1).getLatitude()));
    assertNull(bcoordinates.get(2));
  }

  @Test
  public void georadius() {
    // prepare datas
    Map<String, GeoCoordinate> coordinateMap = new HashMap<String, GeoCoordinate>();
    coordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    coordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    cuckooJedis.geoadd("Sicily", coordinateMap);

    List<GeoRadiusResponse> members = cuckooJedis.georadius("Sicily", 15, 37, 200, GeoUnit.KM);
    assertEquals(2, members.size());

    // sort
    members = cuckooJedis.georadius("Sicily", 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending());
    assertEquals(2, members.size());
    assertEquals("Catania", members.get(0).getMemberByString());
    assertEquals("Palermo", members.get(1).getMemberByString());

    // sort, count 1
    members = cuckooJedis.georadius("Sicily", 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1));
    assertEquals(1, members.size());

    // sort, count 1, withdist, withcoord
    members = cuckooJedis.georadius("Sicily", 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1).withCoord().withDist());
    assertEquals(1, members.size());
    GeoRadiusResponse response = members.get(0);
    assertTrue(equalsWithinEpsilon(56.4413, response.getDistance()));
    assertTrue(equalsWithinEpsilon(15.087269, response.getCoordinate().getLongitude()));
    assertTrue(equalsWithinEpsilon(37.502669, response.getCoordinate().getLatitude()));
  }

  @Test
  public void georadiusReadonly() {
    // prepare datas
    Map<String, GeoCoordinate> coordinateMap = new HashMap<String, GeoCoordinate>();
    coordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    coordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    cuckooJedis.geoadd("Sicily", coordinateMap);

    List<GeoRadiusResponse> members = cuckooJedis.georadiusReadonly("Sicily", 15, 37, 200, GeoUnit.KM);
    assertEquals(2, members.size());

    // sort
    members = cuckooJedis.georadiusReadonly("Sicily", 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending());
    assertEquals(2, members.size());
    assertEquals("Catania", members.get(0).getMemberByString());
    assertEquals("Palermo", members.get(1).getMemberByString());

    // sort, count 1
    members = cuckooJedis.georadiusReadonly("Sicily", 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1));
    assertEquals(1, members.size());

    // sort, count 1, withdist, withcoord
    members = cuckooJedis.georadiusReadonly("Sicily", 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1).withCoord().withDist());
    assertEquals(1, members.size());
    GeoRadiusResponse response = members.get(0);
    assertTrue(equalsWithinEpsilon(56.4413, response.getDistance()));
    assertTrue(equalsWithinEpsilon(15.087269, response.getCoordinate().getLongitude()));
    assertTrue(equalsWithinEpsilon(37.502669, response.getCoordinate().getLatitude()));
  }

  @Test
  public void georadiusBinary() {
    // prepare datas
    Map<byte[], GeoCoordinate> bcoordinateMap = new HashMap<byte[], GeoCoordinate>();
    bcoordinateMap.put(bA, new GeoCoordinate(13.361389, 38.115556));
    bcoordinateMap.put(bB, new GeoCoordinate(15.087269, 37.502669));
    cuckooJedis.geoadd(bfoo, bcoordinateMap);

    List<GeoRadiusResponse> members = cuckooJedis.georadius(bfoo, 15, 37, 200, GeoUnit.KM);
    assertEquals(2, members.size());

    // sort
    members = cuckooJedis.georadius(bfoo, 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending());
    assertEquals(2, members.size());
    assertArrayEquals(bB, members.get(0).getMember());
    assertArrayEquals(bA, members.get(1).getMember());

    // sort, count 1
    members = cuckooJedis.georadius(bfoo, 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1));
    assertEquals(1, members.size());

    // sort, count 1, withdist, withcoord
    members = cuckooJedis.georadius(bfoo, 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1).withCoord().withDist());
    assertEquals(1, members.size());
    GeoRadiusResponse response = members.get(0);
    assertTrue(equalsWithinEpsilon(56.4413, response.getDistance()));
    assertTrue(equalsWithinEpsilon(15.087269, response.getCoordinate().getLongitude()));
    assertTrue(equalsWithinEpsilon(37.502669, response.getCoordinate().getLatitude()));
  }

  @Test
  public void georadiusReadonlyBinary() {
    // prepare datas
    Map<byte[], GeoCoordinate> bcoordinateMap = new HashMap<byte[], GeoCoordinate>();
    bcoordinateMap.put(bA, new GeoCoordinate(13.361389, 38.115556));
    bcoordinateMap.put(bB, new GeoCoordinate(15.087269, 37.502669));
    cuckooJedis.geoadd(bfoo, bcoordinateMap);

    List<GeoRadiusResponse> members = cuckooJedis.georadiusReadonly(bfoo, 15, 37, 200, GeoUnit.KM);
    assertEquals(2, members.size());

    // sort
    members = cuckooJedis.georadiusReadonly(bfoo, 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending());
    assertEquals(2, members.size());
    assertArrayEquals(bB, members.get(0).getMember());
    assertArrayEquals(bA, members.get(1).getMember());

    // sort, count 1
    members = cuckooJedis.georadiusReadonly(bfoo, 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1));
    assertEquals(1, members.size());

    // sort, count 1, withdist, withcoord
    members = cuckooJedis.georadiusReadonly(bfoo, 15, 37, 200, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1).withCoord().withDist());
    assertEquals(1, members.size());
    GeoRadiusResponse response = members.get(0);
    assertTrue(equalsWithinEpsilon(56.4413, response.getDistance()));
    assertTrue(equalsWithinEpsilon(15.087269, response.getCoordinate().getLongitude()));
    assertTrue(equalsWithinEpsilon(37.502669, response.getCoordinate().getLatitude()));
  }

  @Test
  public void georadiusByMember() {
    cuckooJedis.geoadd("Sicily", 13.583333, 37.316667, "Agrigento");
    cuckooJedis.geoadd("Sicily", 13.361389, 38.115556, "Palermo");
    cuckooJedis.geoadd("Sicily", 15.087269, 37.502669, "Catania");

    List<GeoRadiusResponse> members = cuckooJedis.georadiusByMember("Sicily", "Agrigento", 100,
      GeoUnit.KM);
    assertEquals(2, members.size());

    members = cuckooJedis.georadiusByMember("Sicily", "Agrigento", 100, GeoUnit.KM, GeoRadiusParam
        .geoRadiusParam().sortAscending());
    assertEquals(2, members.size());
    assertEquals("Agrigento", members.get(0).getMemberByString());
    assertEquals("Palermo", members.get(1).getMemberByString());

    members = cuckooJedis.georadiusByMember("Sicily", "Agrigento", 100, GeoUnit.KM, GeoRadiusParam
        .geoRadiusParam().sortAscending().count(1).withCoord().withDist());
    assertEquals(1, members.size());

    GeoRadiusResponse member = members.get(0);
    assertEquals("Agrigento", member.getMemberByString());
    assertTrue(equalsWithinEpsilon(0, member.getDistance()));
    assertTrue(equalsWithinEpsilon(13.583333, member.getCoordinate().getLongitude()));
    assertTrue(equalsWithinEpsilon(37.316667, member.getCoordinate().getLatitude()));
  }

  @Test
  public void georadiusByMemberReadonly() {
    cuckooJedis.geoadd("Sicily", 13.583333, 37.316667, "Agrigento");
    cuckooJedis.geoadd("Sicily", 13.361389, 38.115556, "Palermo");
    cuckooJedis.geoadd("Sicily", 15.087269, 37.502669, "Catania");

    List<GeoRadiusResponse> members = cuckooJedis.georadiusByMemberReadonly("Sicily", "Agrigento", 100,
      GeoUnit.KM);
    assertEquals(2, members.size());

    members = cuckooJedis.georadiusByMemberReadonly("Sicily", "Agrigento", 100, GeoUnit.KM, GeoRadiusParam
        .geoRadiusParam().sortAscending());
    assertEquals(2, members.size());
    assertEquals("Agrigento", members.get(0).getMemberByString());
    assertEquals("Palermo", members.get(1).getMemberByString());

    members = cuckooJedis.georadiusByMemberReadonly("Sicily", "Agrigento", 100, GeoUnit.KM, GeoRadiusParam
        .geoRadiusParam().sortAscending().count(1).withCoord().withDist());
    assertEquals(1, members.size());

    GeoRadiusResponse member = members.get(0);
    assertEquals("Agrigento", member.getMemberByString());
    assertTrue(equalsWithinEpsilon(0, member.getDistance()));
    assertTrue(equalsWithinEpsilon(13.583333, member.getCoordinate().getLongitude()));
    assertTrue(equalsWithinEpsilon(37.316667, member.getCoordinate().getLatitude()));
  }

  @Test
  public void georadiusByMemberBinary() {
    cuckooJedis.geoadd(bfoo, 13.583333, 37.316667, bA);
    cuckooJedis.geoadd(bfoo, 13.361389, 38.115556, bB);
    cuckooJedis.geoadd(bfoo, 15.087269, 37.502669, bC);

    List<GeoRadiusResponse> members = cuckooJedis.georadiusByMember(bfoo, bA, 100, GeoUnit.KM);
    assertEquals(2, members.size());

    members = cuckooJedis.georadiusByMember(bfoo, bA, 100, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending());
    assertEquals(2, members.size());
    assertArrayEquals(bA, members.get(0).getMember());
    assertArrayEquals(bB, members.get(1).getMember());

    members = cuckooJedis.georadiusByMember(bfoo, bA, 100, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1).withCoord().withDist());
    assertEquals(1, members.size());

    GeoRadiusResponse member = members.get(0);
    assertArrayEquals(bA, member.getMember());
    assertTrue(equalsWithinEpsilon(0, member.getDistance()));
    assertTrue(equalsWithinEpsilon(13.583333, member.getCoordinate().getLongitude()));
    assertTrue(equalsWithinEpsilon(37.316667, member.getCoordinate().getLatitude()));
  }

  @Test
  public void georadiusByMemberReadonlyBinary() {
    cuckooJedis.geoadd(bfoo, 13.583333, 37.316667, bA);
    cuckooJedis.geoadd(bfoo, 13.361389, 38.115556, bB);
    cuckooJedis.geoadd(bfoo, 15.087269, 37.502669, bC);

    List<GeoRadiusResponse> members = cuckooJedis.georadiusByMemberReadonly(bfoo, bA, 100, GeoUnit.KM);
    assertEquals(2, members.size());

    members = cuckooJedis.georadiusByMemberReadonly(bfoo, bA, 100, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending());
    assertEquals(2, members.size());
    assertArrayEquals(bA, members.get(0).getMember());
    assertArrayEquals(bB, members.get(1).getMember());

    members = cuckooJedis.georadiusByMemberReadonly(bfoo, bA, 100, GeoUnit.KM, GeoRadiusParam.geoRadiusParam()
        .sortAscending().count(1).withCoord().withDist());
    assertEquals(1, members.size());

    GeoRadiusResponse member = members.get(0);
    assertArrayEquals(bA, member.getMember());
    assertTrue(equalsWithinEpsilon(0, member.getDistance()));
    assertTrue(equalsWithinEpsilon(13.583333, member.getCoordinate().getLongitude()));
    assertTrue(equalsWithinEpsilon(37.316667, member.getCoordinate().getLatitude()));
  }

  private void prepareGeoData() {
    Map<String, GeoCoordinate> coordinateMap = new HashMap<String, GeoCoordinate>();
    coordinateMap.put("a", new GeoCoordinate(3, 4));
    coordinateMap.put("b", new GeoCoordinate(2, 3));
    coordinateMap.put("c", new GeoCoordinate(3.314, 2.3241));

    long size = cuckooJedis.geoadd("foo", coordinateMap);
    assertEquals(3, size);

    Map<byte[], GeoCoordinate> bcoordinateMap = new HashMap<byte[], GeoCoordinate>();
    bcoordinateMap.put(bA, new GeoCoordinate(3, 4));
    bcoordinateMap.put(bB, new GeoCoordinate(2, 3));
    bcoordinateMap.put(bC, new GeoCoordinate(3.314, 2.3241));

    size = cuckooJedis.geoadd(bfoo, bcoordinateMap);
    assertEquals(3, size);
  }

  private boolean equalsWithinEpsilon(double d1, double d2) {
    double epsilon = 1E-5;
    return Math.abs(d1 - d2) < epsilon;
  }
}
