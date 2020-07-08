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

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.spark.sql.Row;
import scala.collection.Iterator;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataFrameBulkRecord implements ISQLServerBulkRecord, AutoCloseable, Serializable {
    private static final long serialVersionUID=10l;
    private Iterator<Row> iterator;
    private ColumnMetadata[] dfColumnMetadata;
    private Set<Integer> columnOrdinals;

    public DataFrameBulkRecord(Iterator<Row> iterator, ColumnMetadata[] dfColumnMetadata) {
        this.iterator = iterator;
        this.dfColumnMetadata = dfColumnMetadata;
        this.columnOrdinals = IntStream.range(1, dfColumnMetadata.length+1)
                .boxed().collect(Collectors.toSet());
    }

    @Override
    public Object[] getRowData() throws SQLServerException {

        Row row = iterator.next();
        Object[] rowData = new Object[row.length()];
        for (int idx = 0; idx < dfColumnMetadata.length; idx++) {
             // Columns may be reordered between SQLTable and DataFrame. dfFieldIndex maps to the
             // corresponding column in rowData and thus use dfFieldIndex to get the column.
             int dfFieldIndex = dfColumnMetadata[idx].getDfColIndex();
             rowData[idx] = row.get(dfFieldIndex);
        }
        return rowData;
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        return columnOrdinals;
    }

    @Override
    public String getColumnName(int column) {
        return dfColumnMetadata[column-1].getName();
    }

    @Override
    public int getColumnType(int column) {
        return dfColumnMetadata[column-1].getType();
    }

    @Override
    public int getPrecision(int column) {
        return dfColumnMetadata[column-1].getPrecision();
    }

    @Override
    public int getScale(int column) {
        return dfColumnMetadata[column-1].getScale();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return dfColumnMetadata[column-1].isAutoIncrement();
    }

    @Override
    public boolean next() throws SQLServerException {
        return iterator.hasNext();
    }

    @Override
    public void close() throws SQLServerException {}

    @Override
    public void addColumnMetadata(int positionInFile, String name, int jdbcType,
        int precision, int scale) {}

    @Override
    public void addColumnMetadata(int positionInFile, String name, int jdbcType,
        int precision, int scale, DateTimeFormatter dateTimeFormatter) {}

    @Override
    public DateTimeFormatter getColumnDateTimeFormatter(int column) {
        return null;
    }

    @Override
    public void setTimestampWithTimezoneFormat(String dateTimeFormat) {}

    @Override
    public void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {}

    @Override
    public void setTimeWithTimezoneFormat(String timeFormat) {}

    @Override
    public void	setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {}
}
