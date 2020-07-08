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

import java.io.Serializable;

public class ColumnMetadata implements Serializable {
    private static final long serialVersionUID = 1370047847011926605L;

    private String name;
    private int type;
    private int precision;
    private int scale;
    private boolean isAutoIncrement;
    private int dfColIndex; // index of this column in the dataframe.

    public ColumnMetadata(String name, int type, int precision, int scale,
                          boolean isAutoIncrement, int dfColIndex) {
        this.name = name;
        this.type = type;
        this.precision = precision;
        this.scale = scale;
        this.isAutoIncrement = isAutoIncrement;
        this.dfColIndex = dfColIndex;
    }

    public String getName() {
        return name;
    }

    public int getType() {
        return type;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    public int getDfColIndex() {
        return dfColIndex;
    }
}
