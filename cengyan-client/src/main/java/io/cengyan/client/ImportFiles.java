package io.cengyan.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** 展开 --files，并按文件类型读成列名到文本的行。 */
final class ImportFiles {
    private ImportFiles() { }

    static List<java.nio.file.Path> expand(String spec) throws IOException {
        List<java.nio.file.Path> files = new ArrayList<>();
        for (String piece : spec.split(",")) {
            String text = piece.trim();
            if (text.isEmpty()) continue;
            java.nio.file.Path path = java.nio.file.Path.of(text);
            String name = path.getFileName() == null ? "" : path.getFileName().toString();
            if (name.contains("*") || name.contains("?")) {
                java.nio.file.Path parent = path.getParent() == null ? java.nio.file.Path.of(".") : path.getParent();
                try (DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(parent, name)) {
                    for (java.nio.file.Path match : stream) if (Files.isRegularFile(match)) files.add(match);
                }
            } else {
                if (!Files.isRegularFile(path)) throw new IOException("导入文件不存在：" + path);
                files.add(path);
            }
        }
        if (files.isEmpty()) throw new IOException("没有匹配到导入文件");
        files.sort(java.nio.file.Path::compareTo);
        return files;
    }

    static void read(String type, java.nio.file.Path file, char delimiter, Consumer<Map<String, String>> consumer) throws IOException {
        switch (type) {
            case "tsv" -> readTsv(file, delimiter, consumer);
            case "parquet" -> readParquet(file, consumer);
            case "orc" -> readOrc(file, consumer);
            default -> throw new IOException("不支持的文件类型：" + type);
        }
    }

    static List<String> split(String line, char delimiter) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '"') {
                if (quoted && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else quoted = !quoted;
            } else if (ch == delimiter && !quoted) {
                fields.add(current.toString());
                current.setLength(0);
            } else current.append(ch);
        }
        fields.add(current.toString());
        return fields;
    }

    /** 逐行读，不把整个文件装进内存。 */
    private static void readTsv(java.nio.file.Path file, char delimiter, Consumer<Map<String, String>> consumer) throws IOException {
        try (java.io.BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String first = reader.readLine();
            if (first == null) return;
            List<String> header = split(first, delimiter);
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (line.isBlank()) continue;
                List<String> values = split(line, delimiter);
                Map<String, String> record = new LinkedHashMap<>();
                for (int column = 0; column < header.size(); column++) {
                    record.put(header.get(column), column < values.size() ? values.get(column) : "");
                }
                consumer.accept(record);
            }
        }
    }

    private static void readParquet(java.nio.file.Path file, Consumer<Map<String, String>> consumer) throws IOException {
        Configuration configuration = new Configuration(false);
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file.toUri()))
                .withConf(configuration).build()) {
            Group group;
            while ((group = reader.read()) != null) {
                Map<String, String> record = new LinkedHashMap<>();
                for (Type field : group.getType().getFields()) {
                    String name = field.getName();
                    int index = group.getType().getFieldIndex(name);
                    record.put(name, group.getFieldRepetitionCount(index) == 0 ? "" : group.getValueToString(index, 0));
                }
                consumer.accept(record);
            }
        }
    }

    private static void readOrc(java.nio.file.Path file, Consumer<Map<String, String>> consumer) throws IOException {
        Configuration configuration = new Configuration(false);
        Reader reader = OrcFile.createReader(new Path(file.toUri()), OrcFile.readerOptions(configuration));
        TypeDescription schema = reader.getSchema();
        List<String> names = schema.getFieldNames();
        try (RecordReader rows = reader.rows()) {
            var batch = schema.createRowBatch();
            while (rows.nextBatch(batch)) {
                for (int row = 0; row < batch.size; row++) {
                    Map<String, String> record = new LinkedHashMap<>();
                    for (int column = 0; column < names.size(); column++) record.put(names.get(column), cell(batch.cols[column], row));
                    consumer.accept(record);
                }
            }
        }
    }

    static String cell(ColumnVector column, int row) {
        if (column.isRepeating) row = 0;
        if (!column.noNulls && column.isNull[row]) return "";
        if (column instanceof LongColumnVector values) return Long.toString(values.vector[row]);
        if (column instanceof DoubleColumnVector values) return Double.toString(values.vector[row]);
        if (column instanceof BytesColumnVector values) {
            byte[] bytes = values.vector[row];
            int length = values.length[row];
            if (bytes == null || length <= 0) return "";
            return new String(bytes, values.start[row], length, StandardCharsets.UTF_8);
        }
        if (column instanceof DecimalColumnVector values) return values.vector[row] == null ? "" : values.vector[row].toString();
        if (column instanceof TimestampColumnVector values) return values.asScratchTimestamp(row).toString();
        StringBuilder buffer = new StringBuilder();
        column.stringifyValue(buffer, row);
        return buffer.toString();
    }
}
