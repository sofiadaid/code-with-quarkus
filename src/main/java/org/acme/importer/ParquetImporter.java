package org.acme.importer;

import org.acme.model.Column;
import org.acme.model.DataType;
import org.acme.model.Table;
import org.acme.service.TableRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetImporter {

    public static int loadParquet(File file, String tableName, TableRegistry registry) throws IOException {
        Configuration conf = new Configuration();
        Path hadoopPath = new Path(file.getAbsolutePath());
        int totalInserted = 0;

        try (ParquetFileReader reader =
                     ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, conf))) {

            MessageType schema = reader.getFooter().getFileMetaData().getSchema();

            Table table = registry.get(tableName)
                    .orElseThrow(() -> new IllegalStateException("Table not found: " + tableName));

            // Si la table est vide de colonnes, on les injecte depuis le schéma Parquet
            if (table.columns == null || table.columns.isEmpty()) {
                table.columns = inferColumns(schema);
                table.buildIndex();
            } else if (schema.getFieldCount() != table.columns.size()) {
                throw new IllegalArgumentException(
                        "Schema mismatch: Parquet=" + schema.getFieldCount()
                                + " vs Table=" + table.columns.size());
            }

            List<Column> columns = table.getColumns();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;

            while ((pages = reader.readNextRowGroup()) != null) {
                long rowCount = pages.getRowCount();
                RecordReader<Group> recordReader =
                        columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rowCount; i++) {
                    Group group = recordReader.read();
                    Object[] row = new Object[columns.size()];
                    for (int col = 0; col < columns.size(); col++) {
                        row[col] = readValue(group, columns.get(col), col);
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
            // Accès direct aux champs publics de Column
            Column col = new Column();
            col.name = field.getName();
            col.type = parquetTypeToDataType(field);
            columns.add(col);
        }
        return columns;
    }

    private static DataType parquetTypeToDataType(Type field) {
        if (!field.isPrimitive()) return DataType.STRING;

        switch (field.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32:  return DataType.INT;
            case INT64:  return DataType.LONG;
            case DOUBLE:
            case FLOAT:  return DataType.DOUBLE;
            default:     return DataType.STRING;
        }
    }

    private static Object readValue(Group group, Column column, int index) {
        if (group.getFieldRepetitionCount(index) == 0) return null;

        switch (column.type) {
            case INT:    return group.getInteger(index, 0);
            case LONG:   return group.getLong(index, 0);
            case DOUBLE: return group.getDouble(index, 0);
            case STRING: return group.getString(index, 0);
            default:     return group.getValueToString(index, 0);
        }
    }
}