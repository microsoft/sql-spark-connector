/**
* Copyright 2020 and onwards Microsoft Corporation
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.microsoft.sqlserver.jdbc.spark.bulkwrite;

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
import com.microsoft.sqlserver.jdbc.spark.*;

public class DataSourceUtilsTest {
    @Test
    public void columnMetadataTest() {
        String name = "testName";
        int type = Types.INTEGER;
        int precision = 50;
        int scale = 10;

        ColumnMetadata columnMetadata = new ColumnMetadata(name, type, precision, scale,20);

        assertEquals(name, columnMetadata.getName());
        assertEquals(type, columnMetadata.getType());
        assertEquals(precision, columnMetadata.getPrecision());
        assertEquals(scale, columnMetadata.getScale());
    }

    @Test
    public void dataFrameBulkRecordTest() {
        Row[] rows = new Row[] {
            RowFactory.create(new Object[]{150, "mewtwo"}),
            RowFactory.create(new Object[]{151, "mew"})
        };

        ColumnMetadata[] metadata = new ColumnMetadata[] {
            new ColumnMetadata("entry_number", Types.INTEGER, 10, 5,20),
            new ColumnMetadata("entry_word", Types.LONGVARCHAR, 20, 4,20)
        };

        Iterator<Row> itr = JavaConversions.asScalaIterator(Arrays.asList(rows).iterator());
        DataFrameBulkRecord record = new DataFrameBulkRecord(itr, metadata);

        Set<Integer> columnOrdinals = record.getColumnOrdinals();
        assertTrue(columnOrdinals.contains(1));
        assertTrue(columnOrdinals.contains(2));
        assertTrue(columnOrdinals.size() == 2);

        for (int i = 0; i < metadata.length; i++) {
            assertEquals(record.getColumnName(i+1), metadata[i].getName());
            assertEquals(record.getColumnType(i+1), metadata[i].getType());
            assertEquals(record.getPrecision(i+1), metadata[i].getPrecision());
            assertEquals(record.getScale(i+1), metadata[i].getScale());
        }
    }
}
