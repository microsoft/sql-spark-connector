package com.microsoft.sqlserver.jdbc.spark;

import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.Types;
import java.util.Arrays;
import java.util.Set;
import java.util.NoSuchElementException;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import com.microsoft.sqlserver.jdbc.SQLServerException;

public class DataSourceUtilsTest {
    @Test
    public void columnMetadataTest() {
        String name = "testName";
        int type = Types.INTEGER;
        int precision = 50;
        int scale = 10;
        Boolean isAutoIncrement = true;

        ColumnMetadata columnMetadata = new ColumnMetadata(name, type, precision, scale, isAutoIncrement,20);

        assertEquals(name, columnMetadata.getName());
        assertEquals(type, columnMetadata.getType());
        assertEquals(precision, columnMetadata.getPrecision());
        assertEquals(scale, columnMetadata.getScale());
        assertEquals(isAutoIncrement, columnMetadata.isAutoIncrement());
    }

    @Test
    public void dataFrameBulkRecordTest() {
        Row[] rows = new Row[] {
            RowFactory.create(new Object[]{150, "mewtwo"}),
            RowFactory.create(new Object[]{151, "mew"})
        };

        ColumnMetadata[] metadata = new ColumnMetadata[] {
            new ColumnMetadata("entry_number", Types.INTEGER, 10, 5, true,20),
            new ColumnMetadata("entry_word", Types.LONGVARCHAR, 20, 4, false,20)
        };

        Iterator<Row> itr = JavaConversions.asScalaIterator(Arrays.asList(rows).iterator());
        DataFrameBulkRecord record = new DataFrameBulkRecord(itr, metadata);

        Set<Integer> columnOrdinals = record.getColumnOrdinals();
        assertTrue(columnOrdinals.contains(1));
        assertTrue(columnOrdinals.contains(2));
        assertTrue(columnOrdinals.size() == 2);

        for (int i = 0; i < metadata.length; i++) {
            assertEquals(record.getColumnName(i+1),   metadata[i].getName());
            assertEquals(record.getColumnType(i+1),   metadata[i].getType());
            assertEquals(record.getPrecision(i+1),    metadata[i].getPrecision());
            assertEquals(record.getScale(i+1),        metadata[i].getScale());
            assertEquals(record.isAutoIncrement(i+1), metadata[i].isAutoIncrement());
        }

//        for (int i = 0; i < rows.length; i++) {
//            try {
//                // Assuming that each row has at least two elements
//                //Object[] rowData = record.getRowData();
//                assertEquals(rowData[0], rows[i].get(0));
//                assertEquals(rowData[1], rows[i].get(1));
//            } catch (SQLServerException e) {
//                e.printStackTrace();
//            }
//        }

//        boolean errorCaught = false;
//        try {
//            Object[] rowData = record.getRowData();
//        } catch (SQLServerException e) {
//            e.printStackTrace();
//        } catch (NoSuchElementException e) {
//            errorCaught = true;
//        }
//        assertTrue(errorCaught);
    }
}
