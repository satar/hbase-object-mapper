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

import java.lang.annotation.ElementType;
import java.util.Arrays;

import com.mylife.hbase.mapper.annotation.HBaseField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;

/**
 * A test POJO with all of the supported types. As types are added this model
 * should be updated as well.
 * 
 * @author
 */

@HBasePersistance(tableName = "TEST_MODEL", defaultColumnFamilyName = "STUFF")
public class TestModelOnlyFields {

    @HBaseField
    private Long longField;

    @HBaseField
    @HBaseRowKey
    private String stringField;

    @HBaseField
    private Boolean booleanField;

    @HBaseField
    private byte[] byteArrayField;

    @HBaseField
    private ElementType elementTypeField;

    public Long getLongField() {
        return longField;
    }

    public String getStringField() {
        return stringField;
    }

    public Boolean getBooleanField() {
        return booleanField;
    }

    public byte[] getByteArrayField() {
        return byteArrayField;
    }

    public ElementType getElementTypeField() {
        return elementTypeField;
    }

    public void setElementTypeField(ElementType elementType) {
        this.elementTypeField = elementType;
    }

    public void setLongField(Long longField) {
        this.longField = longField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public void setBooleanField(Boolean booleanField) {
        this.booleanField = booleanField;
    }

    public void setByteArrayField(byte[] byteArrayField) {
        this.byteArrayField = byteArrayField;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((elementTypeField == null) ? 0 : elementTypeField.hashCode());
        result = prime * result + ((booleanField == null) ? 0 : booleanField.hashCode());
        result = prime * result + Arrays.hashCode(byteArrayField);
        result = prime * result + ((longField == null) ? 0 : longField.hashCode());
        result = prime * result + ((stringField == null) ? 0 : stringField.hashCode());
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
        TestModelOnlyFields other = (TestModelOnlyFields) obj;
        if (elementTypeField != other.elementTypeField)
            return false;
        if (booleanField == null) {
            if (other.booleanField != null)
                return false;
        } else if (!booleanField.equals(other.booleanField))
            return false;
        if (!Arrays.equals(byteArrayField, other.byteArrayField))
            return false;
        if (longField == null) {
            if (other.longField != null)
                return false;
        } else if (!longField.equals(other.longField))
            return false;
        if (stringField == null) {
            if (other.stringField != null)
                return false;
        } else if (!stringField.equals(other.stringField))
            return false;
        return true;
    }

    public TestModelOnlyFields(Long longField, String stringField, Boolean booleanField, byte[] byteArrayField,
            ElementType elementType) {
        this.longField = longField;
        this.stringField = stringField;
        this.booleanField = booleanField;
        this.byteArrayField = byteArrayField;
        this.elementTypeField = elementType;
    }


 
}
