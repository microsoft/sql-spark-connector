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

import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class ColumnMetadataTest {
    @Test
    public void checkColumnMetadataTypes() {
        String name = "testName";
        int type = Types.INTEGER;
        int precision = 50;
        int scale = 10;
        Boolean isAutoIncrement = true;

        ColumnMetadata columnMetadata =
                new ColumnMetadata(name, type, precision, scale, isAutoIncrement, 20);

        assertEquals(name, columnMetadata.getName());
        assertEquals(type, columnMetadata.getType());
        assertEquals(precision, columnMetadata.getPrecision());
        assertEquals(scale, columnMetadata.getScale());
        assertEquals(isAutoIncrement, columnMetadata.isAutoIncrement());
    }
}
