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

    public ColumnMetadata(String name, int type, int precision, int scale, boolean isAutoIncrement, int dfColIndex) {
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
