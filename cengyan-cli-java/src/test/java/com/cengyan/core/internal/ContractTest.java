package com.cengyan.core.internal;

import com.cengyan.core.exception.CengyanException;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 契约用例：分片向量由服务端 PartitionRouter 生成，错误码映射由 pycengyan 生成（见 contract-gen/）。
 * Java SDK 必须与服务端、Python SDK 逐条一致。
 */
class ContractTest {
    @SuppressWarnings("unchecked")
    private static Map<String, Object> load(String name) throws IOException {
        try (InputStream in = ContractTest.class.getResourceAsStream("/contract/" + name)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            for (int read; (read = in.read(buffer)) > 0; ) out.write(buffer, 0, read);
            return (Map<String, Object>) Json.decode(out.toByteArray());
        }
    }

    private static byte[] unhex(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        return bytes;
    }

    private static long unsigned(String text) {
        return new BigInteger(text).longValue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void xxhashMatchesServerForEveryLength() throws IOException {
        List<Map<String, Object>> hashes = (List<Map<String, Object>>) load("partition-vectors.json").get("hashes");
        assertEquals(101, hashes.size());
        for (Map<String, Object> vector : hashes) {
            String hex = (String) vector.get("hex");
            assertEquals(unsigned((String) vector.get("hash")), PartitionRouter.hash(unhex(hex)), hex);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void canonicalBytesAndPartitionsMatchServer() throws IOException {
        List<Map<String, Object>> ids = (List<Map<String, Object>>) load("partition-vectors.json").get("ids");
        assertTrue(ids.size() > 100);
        for (Map<String, Object> vector : ids) {
            String type = (String) vector.get("type");
            Object id = vector.get("id");
            String context = type + " " + id;
            assertArrayEquals(unhex((String) vector.get("canonicalHex")), PartitionRouter.canonical(id, type), context);
            assertEquals(unsigned((String) vector.get("hash")), PartitionRouter.hash(PartitionRouter.canonical(id, type)), context);
            for (Map.Entry<String, Object> part : ((Map<String, Object>) vector.get("parts")).entrySet()) {
                assertEquals(((Long) part.getValue()).intValue(), PartitionRouter.partOf(id, type, Integer.parseInt(part.getKey())),
                        context + " / " + part.getKey());
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void errorCodesMapLikePythonSdk() throws IOException {
        List<Map<String, Object>> cases = (List<Map<String, Object>>) load("error-codes.json").get("cases");
        Set<String> covered = new HashSet<String>();
        for (Map<String, Object> item : cases) {
            String code = (String) item.get("code");
            covered.add(code);
            Map<String, Object> response = Collections.<String, Object>singletonMap("code", code);
            assertEquals(item.get("read"), simpleName(ErrorCodes.error(response, false)), code + " read");
            assertEquals(item.get("write"), simpleName(ErrorCodes.error(response, true)), code + " write");
            assertEquals(item.get("unknown"), simpleName(ErrorCodes.error(response, null)), code + " unknown");
        }
        // Java 侧多出来的码说明 Python 侧漏了，反之亦然：两边都必须覆盖。
        assertTrue(covered.containsAll(ErrorCodes.knownCodes()), "Java 映射了但契约里没有的码：" + minus(ErrorCodes.knownCodes(), covered));
    }

    private static String simpleName(CengyanException error) {
        return error.getClass().getSimpleName();
    }

    private static Set<String> minus(Set<String> a, Set<String> b) {
        Set<String> result = new HashSet<String>(a);
        result.removeAll(b);
        return result;
    }
}
