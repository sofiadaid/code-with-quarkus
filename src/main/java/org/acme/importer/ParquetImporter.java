package org.acme.importer;

import org.acme.model.Column;
import org.acme.model.DataType;
import org.acme.model.Table;
import org.acme.service.TableRegistry;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetImporter {

    public static int loadParquet(File file, String tableName, TableRegistry registry) throws IOException {
        int totalInserted = 0;

        InputFile inputFile = new InputFile() {
            @Override
            public long getLength() {
                return file.length();
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                FileInputStream fis = new FileInputStream(raf.getFD());

                return new DelegatingSeekableInputStream(fis) {
                    @Override
                    public long getPos() throws IOException {
                        return raf.getFilePointer();
                    }

                    @Override
                    public void seek(long newPos) throws IOException {
                        raf.seek(newPos);
                    }

                    @Override
                    public void readFully(byte[] bytes) throws IOException {
                        raf.readFully(bytes);
                    }

                    @Override
                    public void readFully(byte[] bytes, int start, int len) throws IOException {
                        raf.readFully(bytes, start, len);
                    }

                    @Override
                    public int read(ByteBuffer byteBuffer) throws IOException {
                        byte[] buffer = new byte[byteBuffer.remaining()];
                        int read = raf.read(buffer);
                        if (read > 0) {
                            byteBuffer.put(buffer, 0, read);
                        }
                        return read;
                    }

                    @Override
                    public void readFully(ByteBuffer byteBuffer) throws IOException {
                        byte[] buffer = new byte[byteBuffer.remaining()];
                        raf.readFully(buffer);
                        byteBuffer.put(buffer);
                    }

                    @Override
                    public void close() throws IOException {
                        fis.close();
                        raf.close();
                    }
                };
            }
        };

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();

            Table table = registry.get(tableName).orElseGet(() -> {
                Table newTable = new Table(tableName, inferColumns(schema));
                return registry.create(newTable);
            });

            List<Column> columns = table.getColumns();

            if (schema.getFieldCount() != columns.size()) {
                throw new IllegalArgumentException(
                        "Schema mismatch: Parquet=" + schema.getFieldCount()
                                + " Table=" + columns.size()
                );
            }

            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;

            while ((pages = reader.readNextRowGroup()) != null) {
                long rowCount = pages.getRowCount();
                RecordReader<Group> recordReader =
                        columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName[] primitiveTypes =
                        new org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName[columns.size()];

                for (int col = 0; col < columns.size(); col++) {
                    Type field = schema.getFields().get(col);
                    primitiveTypes[col] = field.isPrimitive()
                            ? field.asPrimitiveType().getPrimitiveTypeName()
                            : null;
                }

                for (int i = 0; i < rowCount; i++) {
                    Group group = recordReader.read();
                    Object[] row = new Object[columns.size()];

                    for (int col = 0; col < columns.size(); col++) {
                        row[col] = readValue(group, columns.get(col), col, primitiveTypes[col]);
                    }

                    table.addRow(row);
                    totalInserted++;
                }
            }
        }

        return totalInserted;
    }

    private static List<Column> inferColumns(MessageType schema) {
        List<Column> columns = new ArrayList<>();

        for (Type field : schema.getFields()) {
            Column col = new Column();
            col.setName(field.getName());
            col.setType(parquetTypeToDataType(field));
            columns.add(col);
        }

        return columns;
    }

    private static DataType parquetTypeToDataType(Type field) {
        if (!field.isPrimitive()) {
            return DataType.STRING;
        }

        switch (field.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32:
                return DataType.INT;
            case INT64:
                return DataType.LONG;
            case DOUBLE:
                return DataType.DOUBLE;
            case FLOAT:
                return DataType.DOUBLE;
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
                return DataType.STRING;
            default:
                return DataType.STRING;
        }
    }

    private static Object readValue(
            Group group,
            Column column,
            int colIndex,
            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName primitiveType
    ) {
        if (group.getFieldRepetitionCount(colIndex) == 0) {
            return null;
        }

        switch (column.getType()) {
            case INT:
                return group.getInteger(colIndex, 0);
            case LONG:
                return group.getLong(colIndex, 0);
            case DOUBLE:
                if (primitiveType == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT) {
                    return (double) group.getFloat(colIndex, 0);
                }
                return group.getDouble(colIndex, 0);
            case STRING:
                return group.getValueToString(colIndex, 0);
            default:
                return group.getValueToString(colIndex, 0);
        }
    }

    public static Map<String, Object> previewParquet(File file, int limit) throws IOException {
        List<List<Object>> rows = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        int count = 0;

        InputFile inputFile = new InputFile() {
            @Override
            public long getLength() {
                return file.length();
            }

            @Override
            public SeekableInputStream newStream() throws IOException {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                FileInputStream fis = new FileInputStream(raf.getFD());

                return new DelegatingSeekableInputStream(fis) {
                    @Override
                    public long getPos() throws IOException {
                        return raf.getFilePointer();
                    }

                    @Override
                    public void seek(long newPos) throws IOException {
                        raf.seek(newPos);
                    }

                    @Override
                    public void readFully(byte[] bytes) throws IOException {
                        raf.readFully(bytes);
                    }

                    @Override
                    public void readFully(byte[] bytes, int start, int len) throws IOException {
                        raf.readFully(bytes, start, len);
                    }

                    @Override
                    public int read(ByteBuffer byteBuffer) throws IOException {
                        byte[] buffer = new byte[byteBuffer.remaining()];
                        int read = raf.read(buffer);
                        if (read > 0) {
                            byteBuffer.put(buffer, 0, read);
                        }
                        return read;
                    }

                    @Override
                    public void readFully(ByteBuffer byteBuffer) throws IOException {
                        byte[] buffer = new byte[byteBuffer.remaining()];
                        raf.readFully(buffer);
                        byteBuffer.put(buffer);
                    }

                    @Override
                    public void close() throws IOException {
                        fis.close();
                        raf.close();
                    }
                };
            }
        };

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();

            // récupérer les noms de colonnes
            for (Type field : schema.getFields()) {
                columns.add(field.getName());
            }

            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;

            while ((pages = reader.readNextRowGroup()) != null && count < limit) {
                long rowCount = pages.getRowCount();
                RecordReader<Group> recordReader =
                        columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rowCount && count < limit; i++) {
                    Group group = recordReader.read();
                    List<Object> row = new ArrayList<>();

                    for (int col = 0; col < schema.getFieldCount(); col++) {
                        row.add(group.getValueToString(col, 0));
                    }

                    rows.add(row);
                    count++;
                }
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("columns", columns);
        result.put("rows", rows);

        return result;
    }
}