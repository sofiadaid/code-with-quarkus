package org.acme.importer;
import org.acme.model.Column;
import org.acme.model.DataType;
import org.acme.model.Table;
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
import java.util.List;

public class ParquetImporter {

    public static int loadParquet(File file, Table table) throws IOException {

        Configuration conf = new Configuration();
        Path hadoopPath = new Path(file.getAbsolutePath());
        int totalInserted = 0;

        try (ParquetFileReader reader =
                     ParquetFileReader.open(HadoopInputFile.fromPath(hadoopPath, conf))) {

            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            List<Column> columns = table.getColumns();

            // Vérification simple
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
    private static Object readValue(Group group, Column column, int index) {

        if (group.getFieldRepetitionCount(index) == 0) {
            return null;
        }

        DataType type = column.getType();

        switch (type) {
            case INT:
                return group.getInteger(index, 0);

            case LONG:
                return group.getLong(index, 0);

            case DOUBLE:
                return group.getDouble(index, 0);

            case STRING:
                return group.getString(index, 0);

            default:
                return group.getValueToString(index, 0);
        }
    }
}