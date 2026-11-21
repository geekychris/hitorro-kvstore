/*
 * Copyright (c) 2006-2025 Chris Collins
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.hitorro.kvstore;

import tools.jackson.databind.*; import tools.jackson.databind.node.*;
import com.hitorro.kvstore.*; import com.hitorro.kvstore.config.*; import com.hitorro.kvstore.remote.RemoteKvStore;
import com.hitorro.util.core.iterator.sinks.kvstore.*; import com.hitorro.util.core.iterator.sources.kvstore.KvStoreSource;
import java.io.*; import java.math.*; import java.net.*; import java.nio.charset.StandardCharsets; import java.nio.file.*; import java.util.*;
/**
 * Pins the bytes hitorro-kvstore stores and how it reads them to what 3.0.2 did on Jackson 2.18:
 * TypedKVStore values (beans, maps, JsonNode; declaration order, dates as epoch millis, byte[] as
 * base64, BigDecimal as written), reading values 3.x or a hand edit left behind (unknown
 * properties fail, null primitives, trailing tokens, malformed/empty, string numbers...),
 * KvStoreSink keys and rows, KvStoreSource over good and bad values, and RemoteKvStore's
 * get/scan of the fleet endpoint. {@code jackson2-kvstore.txt} is the output of the same code run
 * on the 3.0.2 sources (generator: ~/hitorro-j3/tools/Jackson2KvstoreGolden.java.txt in the
 * migration workspace). {@link #readsBytesWrittenBy3x} also feeds the bytes 3.x stored back
 * through the 4.0.0 TypedKVStore.
 * Jackson 3 node and exception names are mapped back before comparing (StringNode = TextNode).
 */
class Jackson2FormatGoldenTest {
  static final Map<String, String> out = new LinkedHashMap<>();
  static void p(String k, Object v){ out.put(k, String.valueOf(v).replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r")); }
  static boolean jackson(Throwable t){ return t.getClass().getName().startsWith("com.fasterxml.jackson") || t.getClass().getName().startsWith("tools.jackson"); }
  static String desc(Throwable t){ String s = (t instanceof IOException ? "IOE:" : "RTE:") + (jackson(t) ? "jackson" : t.getClass().getSimpleName());
    if (!jackson(t) && t.getCause() != null) s += "<" + (jackson(t.getCause()) ? "jackson" : t.getCause().getClass().getSimpleName()) + ">"; return s; }
  static String tryIt(java.util.concurrent.Callable<Object> c){ try { return String.valueOf(c.call()); } catch(Throwable t){ return "!"+desc(t); } }
  // Result errors carry Jackson's message; keep only the part before it.
  static String res(Result<?> r){ if (r.isSuccess()) { Object v = r.getValue().orElse(null); return "ok:" + (v instanceof byte[] ? new String((byte[]) v, StandardCharsets.UTF_8) : v instanceof JsonNode ? v.getClass().getSimpleName() + " " + v : v); }
    String e = r.getError().orElse(""); int i = e.indexOf(':'); return "fail:" + (i < 0 ? e : e.substring(0, i)); }
  static String types(JsonNode n){ StringBuilder t=new StringBuilder(); if (n == null || !n.isObject()) return ""; Iterator<String> it=n.propertyNames().iterator(); while(it.hasNext()){String k=it.next(); t.append(k+":"+n.get(k).getClass().getSimpleName()+",");} return t.toString(); }
  public static class Doc {
    public String id; public int count; public long big; public double score; public float f; public boolean flag; public Integer boxed;
    public BigDecimal dec; public BigInteger bi; public Date when; public byte[] blob; public char c; public Doc.Kind kind;
    public List<String> tags; public Map<String, Object> attrs; public Doc parent; public String nul;
    private String hidden; public String getHidden(){ return hidden; } public void setHidden(String h){ hidden = h; }
    public enum Kind { A, B }
    public String toString(){ return "Doc[id="+id+",count="+count+",big="+big+",score="+score+",f="+f+",flag="+flag+",boxed="+boxed+",dec="+dec+",bi="+bi+",when="+(when==null?null:when.getTime())+",blob="+(blob==null?null:Arrays.toString(blob))+",c="+(int)c+",kind="+kind+",tags="+tags+",attrs="+(attrs==null?null:new TreeMap<>(attrs))+",parent="+parent+",nul="+nul+",hidden="+hidden+"]"; }
  }
  static Doc doc(String id){ Doc d = new Doc(); d.id = id; d.count = 3; d.big = 12345678901L; d.score = 1.10; d.f = 0.1f; d.flag = true;
    d.dec = new BigDecimal("123456789012345678901234567890.50"); d.bi = new BigInteger("12345678901234567890"); d.when = new Date(1700000000123L);
    d.blob = new byte[]{0, 1, (byte)255}; d.c = 'x'; d.kind = Doc.Kind.B; d.tags = Arrays.asList("a", "\u00e9/\u2028", null);
    d.attrs = new LinkedHashMap<>(); d.attrs.put("z", 1); d.attrs.put("a", 2.5); d.attrs.put("l", Arrays.asList(true, "s")); d.setHidden("h"); return d; }
  static KVStore open(Path p) throws Exception { return new RocksDBStore(DatabaseConfig.builder(p.toString()).compressionType(CompressionType.SNAPPY).storageMode(StorageMode.DISK).createIfMissing(true).build()); }
  public static Map<String, String> compute(Path dir) throws Exception {
    out.clear();
    ObjectMapper om = com.hitorro.util.json.JsonMappers.json();
    // ---- TypedKVStore<Doc>: the bytes it stores, and reading them back
    KVStore raw = open(dir.resolve("typed"));
    TypedKVStore<Doc> ts = new TypedKVStore<>(raw, Doc.class);
    Doc d1 = doc("d1"); Doc d2 = doc("d2"); d2.parent = doc("p"); d2.score = -0.0; d2.dec = new BigDecimal("1E+3"); d2.tags = new ArrayList<>(); d2.attrs = null;
    Doc d3 = new Doc(); d3.id = "d3";
    p("typed.put.d1", res(ts.put("d1", d1))); p("typed.put.d2", res(ts.put(d2, x -> x.id))); p("typed.put.d3", res(ts.put("d3", d3)));
    Map<String, Doc> batch = new TreeMap<>(); batch.put("b1", doc("b1")); batch.put("b2", d3); p("typed.batchPut", res(ts.batchPut(batch, true)));
    for (String k : new String[]{"d1", "d2", "d3", "b1", "b2"}) { p("typed.raw." + k, res(raw.get(k.getBytes(StandardCharsets.UTF_8)))); p("typed.get." + k, res(ts.get(k))); }
    p("typed.get.missing", tryIt(() -> res(ts.get("missing"))));
    p("typed.batchGet", res(ts.batchGet(Arrays.asList("d3", "b2"))));
    p("typed.scan.b", tryIt(() -> { StringBuilder sb = new StringBuilder(); Iterator<Map.Entry<String, Doc>> it = ts.scanByPrefixWithKeys("b"); while (it.hasNext()) { Map.Entry<String, Doc> e = it.next(); sb.append(e.getKey()).append("=").append(e.getValue()).append(" | "); } return sb; }));
    // values 3.x (or a hand edit) left in a store: what get() makes of them
    String[][] legacy = {
      {"unknownProp", "{\"id\":\"u\",\"extra\":1}"}, {"nullPrimitive", "{\"id\":\"n\",\"count\":null,\"flag\":null,\"c\":null}"},
      {"trailing", "{\"id\":\"t\"} {\"x\":1}"}, {"trailingGarbage", "{\"id\":\"t\"} xyz"}, {"malformed", "{\"id\":"}, {"empty", ""}, {"array", "[1]"},
      {"stringNums", "{\"count\":\"7\",\"score\":\"2.5\",\"big\":7.9,\"flag\":\"true\"}"}, {"dateString", "{\"when\":\"2026-01-02T03:04:05.000+00:00\"}"},
      {"dateNumberString", "{\"when\":\"1700000000123\"}"}, {"enumBad", "{\"kind\":\"Z\"}"}, {"enumIndex", "{\"kind\":1}"}, {"singleQuote", "{'id':'q'}"},
      {"comments", "{/*c*/\"id\":\"c\"}"}, {"nan", "{\"score\":NaN}"}, {"blobBad", "{\"blob\":\"***\"}"}, {"emptyStringInt", "{\"count\":\"\"}"}, {"bomUtf8", "\uFEFF{\"id\":\"bom\"}"}};
    for (String[] l : legacy) { raw.put(("L" + l[0]).getBytes(StandardCharsets.UTF_8), l[1].getBytes(StandardCharsets.UTF_8)); p("typed.legacy." + l[0], tryIt(() -> res(ts.get("L" + l[0])))); }
    ts.close();
    // ---- TypedKVStore<Map> and <JsonNode>
    KVStore raw2 = open(dir.resolve("typed2"));
    TypedKVStore<Map> tm = new TypedKVStore<>(raw2, Map.class); TypedKVStore<JsonNode> tn = new TypedKVStore<>(raw2, JsonNode.class);
    Map<String, Object> m = new LinkedHashMap<>(); m.put("z", null); m.put("a", 1.5f); m.put("d", new Date(5)); m.put("n", om.readTree("{\"k\":[1,\"2\",{\"b\":12345678901234567890}]}")); m.put("bd", new BigDecimal("1.10"));
    p("map.put", res(tm.put("m", m))); p("map.raw", res(raw2.get("m".getBytes(StandardCharsets.UTF_8)))); p("map.get", res(tm.get("m")));
    p("node.get", tryIt(() -> { Result<JsonNode> r = tn.get("m"); JsonNode n = r.getValue().orElse(null); return res(r) + " " + types(n) + " " + (n == null ? "" : types(n.get("n").get("k").get(2))); }));
    p("node.put", res(tn.put("n", om.readTree("{\"b\":1.10,\"a\":[],\"big\":12345678901234567890,\"dec\":1.5e300,\"s\":\"\u00e9\"}")))); p("node.raw", res(raw2.get("n".getBytes(StandardCharsets.UTF_8))));
    for (String[] l : legacy) { raw2.put(("L" + l[0]).getBytes(StandardCharsets.UTF_8), l[1].getBytes(StandardCharsets.UTF_8));
      p("node.legacy." + l[0], tryIt(() -> res(tn.get("L" + l[0])))); p("map.legacy." + l[0], tryIt(() -> res(tm.get("L" + l[0])))); }
    tm.close();
    // ---- KvStoreSink rows -> bytes in RocksDB -> KvStoreSource
    Path home = dir.resolve("home");
    KvStoreSink sink = new KvStoreSink("rows", "id", home); sink.start();
    String[] rows = {"{\"id\":\"r1\",\"b\":1.10,\"a\":[1,{\"x\":null}],\"big\":12345678901234567890,\"s\":\"\u00e9/\u2028\"}", "{\"id\":7,\"v\":true}", "{\"id\":{\"o\":1},\"v\":2}",
      "{\"id\":[1,2],\"v\":3}", "{\"id\":null,\"v\":4}", "{\"v\":5}", "{\"id\":2.50,\"v\":6}", "{\"id\":true,\"v\":7}", "{\"id\":\"r1\",\"v\":\"again\"}"};
    for (String r : rows) p("sink.add." + r.hashCode(), tryIt(() -> sink.add(om.readTree(r))));
    p("sink.idem", tryIt(() -> sink.addIdempotent("t", 2, om.readTree("{\"id\":\"i\",\"v\":1}")) + "," + sink.addIdempotent("t", 1, om.readTree("{\"id\":\"i\",\"v\":2}"))));
    KvStoreSink sink2 = new KvStoreSink("rows2", "user.name", home); sink2.start();
    for (String r : new String[]{"{\"user\":{\"name\":\"Al\"},\"k\":1}", "{\"user\":{\"name\":5}}", "{\"user\":null}", "{\"user\":\"str\"}", "{\"user\":{\"name\":{\"x\":1}}}"})
      p("sink2.add." + r.hashCode(), tryIt(() -> sink2.add(om.readTree(r))));
    sink2.close();
    { StringBuilder sb = new StringBuilder(); KvStoreSource src = new KvStoreSource("rows2", home);
      while (src.hasNext()) { sb.append(tryIt(() -> src.next())).append(" | "); } src.close(); p("source.rows2", sb); }
    // raw bad values next to the good ones
    RocksDbSharedRegistry.Entry e = RocksDbSharedRegistry.acquire("rows", home.resolve("kv").resolve("rows"));
    e.db.put("zz-bad".getBytes(StandardCharsets.UTF_8), "{bad".getBytes(StandardCharsets.UTF_8));
    e.db.put("a0-empty".getBytes(StandardCharsets.UTF_8), new byte[0]);
    e.db.put("a1-scalar".getBytes(StandardCharsets.UTF_8), "42 43".getBytes(StandardCharsets.UTF_8));
    RocksDbSharedRegistry.release(e);
    { StringBuilder sb = new StringBuilder(); KvStoreSource src = new KvStoreSource("rows", home); int guard = 0;
      while (src.hasNext() && guard++ < 14) { sb.append(tryIt(() -> { JsonNode n = src.next(); return n.getClass().getSimpleName() + " " + n + " " + types(n); })).append(" | "); }
      src.close(); p("source.rows", sb); }
    sink.close();
    // ---- RemoteKvStore: get bytes and scan pages (NDJSON) from the fleet endpoint
    com.sun.net.httpserver.HttpServer srv = com.sun.net.httpserver.HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    String page = "{\"key\":\"k1\",\"value\":{\"b\":1.10,\"a\":\"\u00e9/\",\"big\":12345678901234567890,\"dec\":1.5e300,\"n\":null}}\n\n{\"key\":2,\"value\":[1,\"x\"]}\nnot json\n{\"key\":null,\"value\":\"s\"}\n{\"key\":{\"o\":1},\"value\":7}\n{\"value\":null}\n{\"key\":\"t\",\"value\":{\"x\":1}} trailing\n{\"key\":\"last\",\"value\":true}";
    srv.createContext("/", ex -> { String path = ex.getRequestURI().getPath(); byte[] resp; int code = 200;
      if (path.contains("/scan")) resp = page.getBytes(StandardCharsets.UTF_8); else if (path.endsWith("/missing")) { code = 404; resp = new byte[0]; } else resp = ("{\"doc\":\"" + path + "\"}").getBytes(StandardCharsets.UTF_8);
      ex.sendResponseHeaders(code, resp.length == 0 ? -1 : resp.length); if (resp.length > 0) ex.getResponseBody().write(resp); ex.close(); });
    srv.start();
    try { RemoteKvStore rk = new RemoteKvStore("http://127.0.0.1:" + srv.getAddress().getPort() + "/", "idx");
      p("remote.get", res(rk.get("a b".getBytes(StandardCharsets.UTF_8)))); p("remote.get.missing", res(rk.get("missing".getBytes(StandardCharsets.UTF_8))));
      p("remote.scan.values", tryIt(() -> { StringBuilder sb = new StringBuilder(); Iterator<byte[]> it = rk.scanByPrefix("k".getBytes(StandardCharsets.UTF_8)); while (it.hasNext()) sb.append(new String(it.next(), StandardCharsets.UTF_8)).append(" | "); return sb; }));
      p("remote.scan.entries", tryIt(() -> { StringBuilder sb = new StringBuilder(); Iterator<Map.Entry<byte[], byte[]>> it = rk.scanByPrefixWithKeys(new byte[0]); while (it.hasNext()) { Map.Entry<byte[], byte[]> x = it.next(); sb.append(new String(x.getKey(), StandardCharsets.UTF_8)).append("=").append(new String(x.getValue(), StandardCharsets.UTF_8)).append(" | "); } return sb; }));
      TypedKVStore<Doc> rt = new TypedKVStore<>(rk, Doc.class);
      p("remote.typed.scan", tryIt(() -> { StringBuilder sb = new StringBuilder(); Iterator<Doc> it = rt.scanByPrefix(""); while (it.hasNext()) sb.append(it.next()).append(" | "); return sb; }));
    } finally { srv.stop(0); }
    return out;
  }

  static Map<String, String> load(String name) throws IOException {
    Map<String, String> m = new LinkedHashMap<>();
    try (InputStream in = Jackson2FormatGoldenTest.class.getResourceAsStream(name)) {
      if (in == null) return m;
      for (String line : new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split("\n")) {
        if (line.isEmpty()) continue;
        int i = line.indexOf('=');
        m.put(line.substring(0, i), line.substring(i + 1));
      }
    }
    return m;
  }

  @org.junit.jupiter.api.Test
  void matchesJackson2Output(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
    Map<String, String> expected = load("jackson2-kvstore.txt");
    
    Map<String, String> actual = new LinkedHashMap<>();
    compute(dir).forEach((k, v) -> actual.put(k, v.replace("StringNode", "TextNode")));
    org.assertj.core.api.Assertions.assertThat(expected).hasSize(99);
    org.assertj.core.api.Assertions.assertThat(actual).containsExactlyEntriesOf(expected);
  }

  /** Undo p(): "\\\\" = backslash, "\\n" = newline, "\\r" = CR. */
  static String unescape(String v) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < v.length(); i++) {
      char c = v.charAt(i);
      if (c == '\\' && i + 1 < v.length()) { char d = v.charAt(++i); sb.append(d == 'n' ? '\n' : d == 'r' ? '\r' : d); } else sb.append(c);
    }
    return sb.toString();
  }

  /** The exact bytes 3.0.2 stored (typed.raw.*) read back by the 4.0.0 TypedKVStore give what 3.0.2 read (typed.get.*). */
  @org.junit.jupiter.api.Test
  void readsBytesWrittenBy3x(@org.junit.jupiter.api.io.TempDir Path dir) throws Exception {
    Map<String, String> expected = load("jackson2-kvstore.txt");
    KVStore raw = open(dir.resolve("legacy"));
    TypedKVStore<Doc> ts = new TypedKVStore<>(raw, Doc.class);
    int n = 0;
    for (Map.Entry<String, String> e : expected.entrySet()) {
      if (!e.getKey().startsWith("typed.raw.")) continue;
      String key = e.getKey().substring("typed.raw.".length());
      String json = unescape(e.getValue().substring("ok:".length()));
      raw.put(key.getBytes(StandardCharsets.UTF_8), json.getBytes(StandardCharsets.UTF_8));
      org.assertj.core.api.Assertions.assertThat(res(ts.get(key))).isEqualTo(expected.get("typed.get." + key));
      n++;
    }
    org.assertj.core.api.Assertions.assertThat(n).isEqualTo(5);
    ts.close();
  }
}
