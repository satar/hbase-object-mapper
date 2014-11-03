// =======================================================
// Copyright Mylife.com Inc., 2013. All rights reserved.
//
// =======================================================
//  Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.mylife.hbase.mapper.model;

import java.util.ArrayList;
import java.util.List;

import com.mylife.hbase.mapper.annotation.HBaseField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;

/**
 * A test POJO with one unsupported type.
 * 
 * @author Mike E
 */
@HBasePersistance(tableName = "TEST_MODEL", defaultColumnFamilyName = "STUFF")
public class TestModelWithUnsupportedTypeAnnotated {

    @HBaseField
    private Short shortField;

    @HBaseField
    private List<Short> shorts = new ArrayList<Short>();

    public Short getShortField() {
        return shortField;
    }

    public List<Short> getShorts() {
        return shorts;
    }

    public TestModelWithUnsupportedTypeAnnotated(Short shortField, List<Short> shorts) {
        super();
        this.shortField = shortField;
        this.shorts = shorts;
    }

    @Override
    public String toString() {
        return "TestModelWithUnsupportedTypeAnnotated [shortField=" + shortField + ", shorts=" + shorts + "]";
    }

    @HBaseRowKey
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((shortField == null) ? 0 : shortField.hashCode());
        result = prime * result + ((shorts == null) ? 0 : shorts.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TestModelWithUnsupportedTypeAnnotated other = (TestModelWithUnsupportedTypeAnnotated) obj;
        if (shortField == null) {
            if (other.shortField != null)
                return false;
        } else if (!shortField.equals(other.shortField))
            return false;
        if (shorts == null) {
            if (other.shorts != null)
                return false;
        } else if (!shorts.equals(other.shorts))
            return false;
        return true;
    }
}