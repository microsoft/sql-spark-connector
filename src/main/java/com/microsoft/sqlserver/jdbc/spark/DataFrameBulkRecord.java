 /*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
