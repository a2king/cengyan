package io.cengyan.studio.cluster;

import io.cengyan.studio.cluster.ClusterState.Role;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PackagesTest {
    @TempDir Path tmp;
    Bundle bundle;
    Packages packages;

    @BeforeEach void open() throws Exception {
        Path home = Fakes.home(tmp.resolve("home"));
        bundle = new Bundle(home);
        packages = new Packages(home.resolve("data/packages"), home.resolve("inbox"));
    }

    @Test void bundledFilesBecomeThePackageNodesAlreadyRun() throws Exception {
        Packages.Package bundled = packages.importBundle(bundle);
        assertEquals(bundle.releaseId(), bundled.id(), "ID 必须和以前随附程序算出的目录名一致，已部署的节点才认得");
        assertEquals("1.0.0", bundled.version());
        assertEquals(bundled.id(), packages.importBundle(bundle).id(), "重复登记不产生新程序包");
        assertEquals(1, packages.list().size());
    }

    @Test void singleComputeJarKeepsStorageJarFromLatest() throws Exception {
        Packages.Package base = packages.importBundle(bundle);
        Fakes.nodeJar(packages.inbox().resolve("cengyan-compute-fix.jar"), Role.COMPUTE, "fix");
        List<Packages.ScanResult> results = packages.scan();
        assertEquals(1, results.size());
        assertTrue(results.getFirst().ok(), results.getFirst().message());
        Packages.Package next = packages.require(results.getFirst().release());
        assertEquals(next.id(), packages.latest().id());
        assertEquals(base.files().get("cengyan-storage.jar"), next.files().get("cengyan-storage.jar"));
        assertNotEquals(base.files().get("cengyan-compute.jar"), next.files().get("cengyan-compute.jar"));
        assertTrue(packages.differs(base.id(), next.id(), Role.COMPUTE));
        assertFalse(packages.differs(base.id(), next.id(), Role.STORAGE), "storage jar 没变，存储节点不需要升级");
        assertFalse(Files.exists(packages.inbox().resolve("cengyan-compute-fix.jar")), "导入成功后清理导入目录");
        assertEquals(next.files().get("cengyan-compute.jar"), Bundle.sha256(packages.file(next.id(), "cengyan-compute.jar")));
    }

    @Test void archiveCarriesItsOwnVersion() throws Exception {
        packages.importBundle(bundle);
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("cengyan-1.0.1/BUILD-INFO.json", "{\"version\":\"1.0.1\"}".getBytes(StandardCharsets.UTF_8));
        entries.put("cengyan-1.0.1/nodes/cengyan-storage.jar", Files.readAllBytes(Fakes.nodeJar(tmp.resolve("s.jar"), Role.STORAGE, "s2")));
        entries.put("cengyan-1.0.1/nodes/cengyan-compute.jar", Files.readAllBytes(Fakes.nodeJar(tmp.resolve("c.jar"), Role.COMPUTE, "c2")));
        entries.put("cengyan-1.0.1/nodes/logback.xml", "<configuration/>".getBytes(StandardCharsets.UTF_8));
        entries.put("cengyan-1.0.1/cengyan-studio.jar", new byte[2000]);
        Path archive = Fakes.tarGz(packages.inbox().resolve("cengyan-1.0.1-linux.tar.gz"), entries);
        Files.writeString(archive.resolveSibling(archive.getFileName() + ".sha256"), Bundle.sha256(archive) + "  " + archive.getFileName() + "\n");
        Packages.ScanResult result = packages.scan().getFirst();
        assertTrue(result.ok(), result.message());
        Packages.Package imported = packages.require(result.release());
        assertEquals("1.0.1", imported.version());
        assertTrue(imported.id().startsWith("1.0.1-"));
        assertTrue(packages.differs(bundle.releaseId(), imported.id(), Role.STORAGE));
        assertEquals("<configuration/>", Files.readString(packages.file(imported.id(), "logback.xml")));
    }

    @Test void badFilesAreSetAsideWithTheReason() throws Exception {
        packages.importBundle(bundle);
        Path inbox = packages.inbox();
        Fakes.old(Files.writeString(inbox.resolve("broken.jar"), "not a jar"));
        Fakes.nodeJar(inbox.resolve("studio.jar"), Role.COMPUTE, "x");
        Fakes.old(Files.writeString(inbox.resolve("studio.jar.sha256"), "0".repeat(64)));
        Fakes.nodeJar(inbox.resolve("uploading.jar"), Role.COMPUTE, "y");
        Files.setLastModifiedTime(inbox.resolve("uploading.jar"), java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis()));
        Files.writeString(inbox.resolve("cengyan-compute.jar.part"), "half");
        Map<String, Packages.ScanResult> results = new LinkedHashMap<>();
        for (Packages.ScanResult r : packages.scan()) results.put(r.file(), r);
        assertEquals(List.of("broken.jar", "studio.jar", "uploading.jar"), List.copyOf(results.keySet()), ".part 文件不处理");
        assertFalse(results.get("broken.jar").ok());
        assertTrue(results.get("studio.jar").message().contains("SHA-256"), results.get("studio.jar").message());
        assertTrue(results.get("uploading.jar").message().contains("没有传完"), results.get("uploading.jar").message());
        assertTrue(Files.exists(inbox.resolve("broken.jar.failed")));
        assertTrue(Files.exists(inbox.resolve("uploading.jar")), "没传完的文件留着下次处理");
        assertEquals(1, packages.list().size(), "失败的文件不产生程序包");
    }

    @Test void jarWithoutNativeLibrariesIsRejected() throws Exception {
        Path jar = tmp.resolve("thin.jar");
        var manifest = new java.util.jar.Manifest();
        manifest.getMainAttributes().put(java.util.jar.Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(java.util.jar.Attributes.Name.MAIN_CLASS, "io.cengyan.compute.ComputeServer");
        try (var out = new java.util.jar.JarOutputStream(Files.newOutputStream(jar), manifest)) { out.putNextEntry(new java.util.zip.ZipEntry("a.txt")); }
        ClusterError error = assertThrows(ClusterError.class, () -> Packages.verifyJar(jar));
        assertTrue(error.getMessage().contains("本地库"), error.getMessage());
    }
}
