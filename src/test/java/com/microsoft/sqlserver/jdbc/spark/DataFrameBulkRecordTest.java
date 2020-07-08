/*
 * Copyright (c) 2018 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.microsoft.sqlserver.jdbc.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Test;
import scala.collection.Iterator;
import scala.collection.JavaConversions;

import java.sql.Types;
import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class  DataFrameBulkRecordTest {

    @Test
    public void testRecords() {
        Row[] rows = new Row[]{
                RowFactory.create(new Object[]{150, "mewtwo"}),
                RowFactory.create(new Object[]{151, "mew"})
        };

        ColumnMetadata[] metadata = new ColumnMetadata[]{
                new ColumnMetadata("entry_number", Types.INTEGER, 10, 5, true, 20),
                new ColumnMetadata("entry_word", Types.LONGVARCHAR, 20, 4, false, 20)
        };

        Iterator<Row> itr = JavaConversions.asScalaIterator(Arrays.asList(rows).iterator());
        DataFrameBulkRecord record = new DataFrameBulkRecord(itr, metadata);

        Set<Integer> columnOrdinals = record.getColumnOrdinals();
        assertTrue(columnOrdinals.contains(1));
        assertTrue(columnOrdinals.contains(2));
        assertTrue(columnOrdinals.size() == 2);

        for (int i = 0; i < metadata.length; i++) {
            assertEquals(record.getColumnName(i + 1), metadata[i].getName());
            assertEquals(record.getColumnType(i + 1), metadata[i].getType());
            assertEquals(record.getPrecision(i + 1), metadata[i].getPrecision());
            assertEquals(record.getScale(i + 1), metadata[i].getScale());
            assertEquals(record.isAutoIncrement(i + 1), metadata[i].isAutoIncrement());
        }
    }
}
