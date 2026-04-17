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
import java.util.List;

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

            // APRÈS
            Table table = registry.get(tableName).orElseGet(() -> {
                Table newTable = new Table(tableName, inferColumns(schema));

                if (newTable.getData().containsKey("payment_type")) {
                    newTable.addIndexedColumn("payment_type");
                }
                if (newTable.getData().containsKey("vendor_id")) {
                    newTable.addIndexedColumn("vendor_id");
                }

                return registry.create(newTable);
            });

// Si la table existe mais est vide (créée sans colonnes), on injecte le schéma Parquet
            if (table.getColumns().isEmpty()) {
                List<Column> inferredColumns = inferColumns(schema);
                table.setColumns(inferredColumns);
                table.buildIndex();
                table.initializeStorage();
            }

            List<Column> columns = table.getColumns();

            if (schema.getFieldCount() != columns.size()) {
                throw new IllegalArgumentException(
                        "Schema mismatch: Parquet=" + schema.getFieldCount()
                                + " Table=" + columns.size()
                );
            }
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;

            org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName[] primitiveTypes =
                    new org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName[columns.size()];

            for (int col = 0; col < columns.size(); col++) {
                Type field = schema.getFields().get(col);
                primitiveTypes[col] = field.isPrimitive()
                        ? field.asPrimitiveType().getPrimitiveTypeName()
                        : null;
            }

            while ((pages = reader.readNextRowGroup()) != null) {
                long rowCount = pages.getRowCount();

                RecordReader<Group> recordReader =
                        columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rowCount; i++) {
                    Group group = recordReader.read();
                    Object[] row = new Object[columns.size()];

                    for (int col = 0; col < columns.size(); col++) {
                        row[col] = readValue(group, columns.get(col), col, primitiveTypes[col]);
                    }

                    table.addRow(row);
                    totalInserted++;

                    if (totalInserted % 100_000 == 0) {
                        System.out.println("Imported rows: " + totalInserted);
                    }
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

        return switch (field.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> DataType.INT;
            case INT64 -> DataType.LONG;
            case DOUBLE -> DataType.DOUBLE;
            case FLOAT -> DataType.DOUBLE;
            case BINARY, FIXED_LEN_BYTE_ARRAY -> DataType.STRING;
            default -> DataType.STRING;
        };
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

        return switch (column.getType()) {
            case INT -> group.getInteger(colIndex, 0);
            case LONG -> group.getLong(colIndex, 0);
            case DOUBLE -> {
                if (primitiveType == org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT) {
                    yield (double) group.getFloat(colIndex, 0);
                }
                yield group.getDouble(colIndex, 0);
            }
            case STRING -> group.getValueToString(colIndex, 0);
            default -> group.getValueToString(colIndex, 0);
        };
    }

    public static List<Object[]> previewParquet(File file, int limit) throws IOException {
        List<Object[]> preview = new ArrayList<>();
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
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;

            while ((pages = reader.readNextRowGroup()) != null && count < limit) {
                long rowCount = pages.getRowCount();
                RecordReader<Group> recordReader =
                        columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rowCount && count < limit; i++) {
                    Group group = recordReader.read();
                    Object[] row = new Object[schema.getFieldCount()];

                    for (int col = 0; col < schema.getFieldCount(); col++) {
                        row[col] = group.getValueToString(col, 0);
                    }

                    preview.add(row);
                    count++;
                }
            }
        }

        return preview;
    }
}