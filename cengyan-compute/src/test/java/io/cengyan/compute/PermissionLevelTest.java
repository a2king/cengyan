package io.cengyan.compute;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cengyan.distributed.ClusterException;
import io.cengyan.distributed.TokenAuthority;
import org.junit.jupiter.api.Test;

import static io.cengyan.distributed.TokenAuthority.*;
import static org.junit.jupiter.api.Assertions.*;

/** docs/user-permissions.md 第 3 节的权限矩阵。 */
class PermissionLevelTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private static int gql(String query) { return ComputeServer.requiredLevel(JSON.createObjectNode().put("op", "execute").put("query", query)); }
    private static int admin(String command) { return ComputeServer.requiredLevel(JSON.createObjectNode().put("op", "admin").put("command", command)); }
    private static int op(String op) { return ComputeServer.requiredLevel(JSON.createObjectNode().put("op", op)); }

    @Test
    void 只读语句为read() {
        for (String q : new String[] {"MATCH (a:Company) RETURN a.id", "SHOW GRAPHS", "SHOW NODE TYPES", "DESCRIBE NODE TYPE Company", "USE g",
                "EXPLAIN MATCH (a:Company) RETURN a.id", "CALL path.shortest({start:'a', end:'b', rel:'holder', hops:3}) YIELD length, vids",
                "MATCH (a:Company {id:'a'}) RETURN a.id AS id UNION MATCH (a:Company {id:'b'}) RETURN a.id AS id", "RETURN 1+2 AS answer"}) {
            assertEquals(READ, gql(q), q);
        }
    }

    @Test
    void 改数据语句为write() {
        for (String q : new String[] {"INSERT (n:Company {id:'x', name:'n'})", "MATCH (a:Company {id:'b'}) SET a.name='Changed'",
                "MATCH (a:Company {id:'b'}) DETACH DELETE a", "INSERT (a {id:'b'})-[:holder {id:'e2'}]->(b {id:'a'})"}) {
            assertEquals(WRITE, gql(q), q);
        }
    }

    @Test
    void 所有DDL只给admin() {
        for (String q : new String[] {"CREATE GRAPH g", "DROP GRAPH g", "CREATE ALIAS a FOR GRAPH g", "DROP ALIAS a",
                "CREATE NODE TYPE N ID STRING(64) (body STRING NULL)", "CREATE REL TYPE holder ()", "DROP NODE TYPE N",
                "CREATE INDEX by_name ON NODE TYPE Company (name)", "DROP INDEX by_name"}) {
            assertEquals(ADMIN, gql(q), q);
        }
    }

    @Test
    void 语法错误直接返回SYNTAX() {
        ClusterException ex = assertThrows(ClusterException.class, () -> gql("MATCH (a RETURN"));
        assertEquals("SYNTAX", ex.code());
    }

    @Test
    void 管理命令和其他操作() {
        for (String c : new String[] {"stats", "storage", "showNodes", "showMembers", "computeLoad", "maintenanceStatus", "changePassword"}) assertEquals(READ, admin(c), c);
        for (String c : new String[] {"addReplica", "drainStorage", "removeStorage", "drainCompute", "resumeCompute", "removeCompute",
                "maintenanceEnter", "maintenanceExit", "transferMetadataLeader", "listUsers", "createUser", "setRole", "setPassword", "dropUser",
                "forceDropGraph", "repairGraphCreate", "某个以后新增的命令"}) assertEquals(ADMIN, admin(c), c);
        assertEquals(WRITE, op("bulkInsert"));
        assertEquals(WRITE, op("importPrepare"));
        assertEquals(READ, op("bulkTopology"));
        assertEquals(ADMIN, op("createGraph"));
        assertEquals(ADMIN, op("某个以后新增的操作"));
    }

    @Test
    void root按名字判定为admin() {
        assertEquals(ADMIN, TokenAuthority.level("root", "write"));
        assertEquals("admin", TokenAuthority.effectiveRole("root", "write"));
        assertEquals(WRITE, TokenAuthority.level("w", "write"));
        assertEquals(0, TokenAuthority.level("x", "superuser"));
    }
}
