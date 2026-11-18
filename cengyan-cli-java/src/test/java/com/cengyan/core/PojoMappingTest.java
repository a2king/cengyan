package com.cengyan.core;

import com.cengyan.core.exception.BulkInsertException;
import com.cengyan.core.exception.ResultException;
import com.cengyan.core.mapping.CengyanId;
import com.cengyan.core.mapping.CengyanIgnore;
import com.cengyan.core.mapping.CengyanProperty;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.cengyan.core.ResultTest.map;
import static com.cengyan.core.ResultTest.node;
import static com.cengyan.core.ResultTest.result;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PojoMappingTest {
    static class Base {
        @CengyanId
        private long key;
    }

    static class Person extends Base {
        private String name;
        @CengyanProperty("full_age")
        private int age;
        private Double score;
        private Instant born;
        private LocalDateTime seen;
        private TimeUnit unit;
        private List<String> tags;
        private Set<Long> codes;
        private Map<String, Object> extra;
        @CengyanIgnore
        private String ignored = "keep";
        private transient String scratch = "keep";
        private static String shared = "keep";

        private Person() {
        }

        Person(long key, String name, int age) {
            ((Base) this).key = key;
            this.name = name;
            this.age = age;
        }
    }

    static class Knows {
        String id;
        String src;
        String dst;
        long since;
    }

    static class Plain {
        long id;
        String name;
    }

    static class NoId {
        String name;
    }

    static class TwoIds {
        @CengyanId
        long a;
        long id;
    }

    static class NoDefaultConstructor {
        long id;

        NoDefaultConstructor(long id) {
            this.id = id;
        }
    }

    @Test
    void columnsMapToFieldsWithRenameConversionAndIgnore() {
        Result result = result(Arrays.asList("key", "name", "full_age", "score", "born", "seen", "unit", "tags", "codes", "extra", "ignored", "unknown"),
                Arrays.asList(7L, "张三", 30L, 2L, "2026-11-10T08:30:00.123Z", "2026-11-10 08:30:00", "SECONDS",
                        Arrays.asList("a", "b"), Arrays.asList(1L, 1L, 2L), map("k", 1L), "server", "dropped"));
        Person person = result.one(Person.class);
        assertEquals(0L, ((Base) person).key, "@CengyanId 字段只对应 id，不对应 key 列");
        assertEquals("张三", person.name);
        assertEquals(30, person.age);
        assertEquals(2.0, person.score);
        assertEquals(Instant.parse("2026-11-10T08:30:00.123Z"), person.born);
        assertEquals(LocalDateTime.of(2026, 11, 10, 8, 30), person.seen);
        assertEquals(TimeUnit.SECONDS, person.unit);
        assertEquals(Arrays.asList("a", "b"), person.tags);
        assertEquals(2, person.codes.size());
        assertEquals(map("k", 1L), person.extra);
        assertEquals("keep", person.ignored);
        assertEquals("keep", person.scratch);
    }

    @Test
    void wholeNodeIsExpandedIntoIdAndProperties() {
        Result result = result(Collections.singletonList("n"),
                Collections.singletonList(node(42L, "Person", map("name", "A", "full_age", 5L))),
                Collections.singletonList(node(43L, "Person", map("name", "B"))));
        List<Person> people = result.list(Person.class);
        assertEquals(42L, ((Base) people.get(0)).key);
        assertEquals("A", people.get(0).name);
        assertEquals(5, people.get(0).age);
        assertEquals(0, people.get(1).age, "缺的属性保持默认值");

        Plain plain = result(Collections.singletonList("n"), Collections.singletonList(node(9L, "P", map("name", "x")))).one(Plain.class);
        assertEquals(9L, plain.id);
    }

    @Test
    void wholeEdgeIsExpandedWithEndpoints() {
        Map<String, Object> edge = map("id", "e1", "type", "knows", "properties", map("since", 2020L), "src", "a", "dst", "b");
        Knows knows = result(Collections.singletonList("e"), Collections.singletonList(edge)).one(Knows.class);
        assertEquals("e1", knows.id);
        assertEquals("a", knows.src);
        assertEquals("b", knows.dst);
        assertEquals(2020L, knows.since);
    }

    @Test
    void singleColumnValueTypes() {
        Result names = result(Collections.singletonList("name"), Collections.singletonList("a"), Collections.singletonList("b"));
        assertEquals(Arrays.asList("a", "b"), names.list(String.class));
        assertEquals(3L, (long) result(Collections.singletonList("c"), Collections.singletonList(3L)).one(Long.class));
        assertEquals(1L, result(Collections.singletonList("n"), Collections.singletonList(node(1L, "P", map()))).one(GraphNode.class).id());
        assertThrows(ResultException.class, () -> result(Arrays.asList("a", "b"), Arrays.asList(1L, 2L)).one(Long.class));
    }

    @Test
    void conversionErrorsAreResultExceptions() {
        ResultException overflow = assertThrows(ResultException.class,
                () -> result(Collections.singletonList("full_age"), Collections.singletonList(1L << 40)).one(Person.class));
        assertEquals("RESULT_MAPPING", overflow.code());
        assertTrue(overflow.getMessage().contains("Person.age"), overflow.getMessage());
        assertThrows(ResultException.class, () -> result(Collections.singletonList("name"), Collections.singletonList(1L)).one(Person.class));
        assertThrows(ResultException.class, () -> result(Collections.singletonList("unit"), Collections.singletonList("NOPE")).one(Person.class));
        assertThrows(ResultException.class, () -> result(Collections.singletonList("id"), Collections.singletonList(1L)).one(NoDefaultConstructor.class));
        assertThrows(ResultException.class, () -> result(Collections.singletonList("id"), Collections.singletonList(1L)).one(TwoIds.class));
    }

    @Test
    void nullDoesNotOverwritePrimitiveDefaults() {
        Person person = result(Arrays.asList("full_age", "name"), Arrays.asList(null, null)).one(Person.class);
        assertEquals(0, person.age);
        assertNull(person.name);
    }

    @Test
    @SuppressWarnings("unchecked")
    void pojoRowsAreWrittenWithIdAndProperties() {
        BulkInsertTest.FakeCluster cluster = new BulkInsertTest.FakeCluster();
        Person person = new Person(7L, "张三", 30);
        cluster.graph().insertNodes("Person", Collections.singletonList(person), BulkInsertTest.sequential(10));
        Map<String, Object> row = ((List<Map<String, Object>>) cluster.bulks.get(0).get("rows")).get(0);
        assertEquals(Arrays.asList("id", "name", "full_age", "score", "born", "seen", "unit", "tags", "codes", "extra"),
                new java.util.ArrayList<String>(row.keySet()));
        assertEquals(7L, row.get("id"));
        assertEquals(30, row.get("full_age"));

        Knows knows = new Knows();
        knows.id = "e1";
        knows.src = "a";
        knows.dst = "b";
        cluster.graph().insertEdges("knows", Collections.singletonList(knows));
        Map<String, Object> edge = ((List<Map<String, Object>>) cluster.bulks.get(1).get("rows")).get(0);
        assertEquals(Arrays.asList("id", "src", "dst", "since"), new java.util.ArrayList<String>(edge.keySet()));
    }

    @Test
    void alignedWritesHashPojoIds() {
        BulkInsertTest.FakeCluster cluster = new BulkInsertTest.FakeCluster();
        List<Person> people = Arrays.asList(new Person(1, "a", 1), new Person(2, "b", 2), new Person(3, "c", 3));
        assertEquals(3, cluster.graph().insertNodes("Person", people).confirmed());
    }

    @Test
    void misconfiguredPojosAreRejectedBeforeAnyWrite() {
        BulkInsertTest.FakeCluster cluster = new BulkInsertTest.FakeCluster();
        BulkInsertException noId = assertThrows(BulkInsertException.class,
                () -> cluster.graph().insertNodes("N", Collections.singletonList(new NoId()), BulkInsertTest.sequential(1)));
        assertTrue(noId.getMessage().contains("主键"), noId.getMessage());
        Plain plain = new Plain();
        assertThrows(BulkInsertException.class, () -> cluster.graph().insertEdges("E", Collections.singletonList(plain)));
        assertThrows(BulkInsertException.class, () -> cluster.graph().insertNodes("N", Collections.singletonList("not a row")));
        assertTrue(cluster.bulks.isEmpty());
    }
}
