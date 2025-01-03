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

package com.microsoft.sqlserver.jdbc.spark;

import java.io.Serializable;

public class ColumnMetadata implements Serializable {
    private static final long serialVersionUID = 1370047847011926605L;

    private String name;
    private int type;
    private int precision;
    private int scale;
    private int dfColIndex; // index of this column in the dataframe.

    public ColumnMetadata(String name, int type, int precision, int scale, int dfColIndex) {
        this.name = name;
        this.type = type;
        this.precision = precision;
        this.scale = scale;
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

    public int getDfColIndex() {
        return dfColIndex;
    }
}
