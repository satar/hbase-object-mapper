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
import java.util.HashMap;

import com.mylife.hbase.mapper.annotation.HBaseField;
import com.mylife.hbase.mapper.annotation.HBaseMapField;
import com.mylife.hbase.mapper.annotation.HBaseObjectField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;

/**
 * A test POJO with some valid types and a good map backing object
 * 
 * @author Mike E
 */
@HBasePersistance(tableName = "TEST_MODEL", defaultColumnFamilyName = "STUFF")
public class TestModelWithGoodHashMap {

    @HBaseField(columnFamilyName = "OTHER_STUFF")
    @HBaseRowKey
    private Long longField;

    @HBaseField
    private String stringField;

    @HBaseField(columnFamilyName = "MORE_STUFF")
    private Boolean booleanField;

    @HBaseField(columnFamilyName = "OTHER_STUFF")
    private byte[] byteArrayField;

    @HBaseField
    private ElementType elementTypeField;

    @HBaseObjectField(columnFamilyName = "OBJECT_STUFF")
    private LabeledPoint labeledPoint;

    @HBaseMapField(columnFamilyName = "MAP_STUFF")
    private HashMap<String, String> goodMap;

    public Long getLongField() {
        return longField;
    }

    public void setLongField(Long longField) {
        this.longField = longField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public Boolean getBooleanField() {
        return booleanField;
    }

    public void setBooleanField(Boolean booleanField) {
        this.booleanField = booleanField;
    }

    public byte[] getByteArrayField() {
        return byteArrayField;
    }

    public void setByteArrayField(byte[] byteArrayField) {
        this.byteArrayField = byteArrayField;
    }

    public ElementType getElementTypeField() {
        return elementTypeField;
    }

    public void setElementTypeField(ElementType elementTypeField) {
        this.elementTypeField = elementTypeField;
    }

    public LabeledPoint getLabeledPoint() {
        return labeledPoint;
    }

    public void setLabeledPoint(LabeledPoint labeledPoint) {
        this.labeledPoint = labeledPoint;
    }

    public HashMap<String, String> getGoodMap() {
        return goodMap;
    }

    public void setGoodMap(HashMap<String, String> goodMap) {
        this.goodMap = goodMap;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((booleanField == null) ? 0 : booleanField.hashCode());
        result = prime * result + Arrays.hashCode(byteArrayField);
        result = prime * result + ((elementTypeField == null) ? 0 : elementTypeField.hashCode());
        result = prime * result + ((goodMap == null) ? 0 : goodMap.hashCode());
        result = prime * result + ((labeledPoint == null) ? 0 : labeledPoint.hashCode());
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
        TestModelWithGoodHashMap other = (TestModelWithGoodHashMap) obj;
        if (booleanField == null) {
            if (other.booleanField != null)
                return false;
        } else if (!booleanField.equals(other.booleanField))
            return false;
        if (!Arrays.equals(byteArrayField, other.byteArrayField))
            return false;
        if (elementTypeField != other.elementTypeField)
            return false;
        if (goodMap == null) {
            if (other.goodMap != null)
                return false;
        } else if (!goodMap.equals(other.goodMap))
            return false;
        if (labeledPoint == null) {
            if (other.labeledPoint != null)
                return false;
        } else if (!labeledPoint.equals(other.labeledPoint))
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

    @Override
    public String toString() {
        return "TestModelWithGoodHashMap [longField=" + longField + ", stringField=" + stringField + ", booleanField="
                + booleanField + ", byteArrayField=" + Arrays.toString(byteArrayField) + ", elementTypeField="
                + elementTypeField + ", labeledPoint=" + labeledPoint + ", goodMap=" + goodMap + "]";
    }

    public TestModelWithGoodHashMap(Long longField, String stringField, Boolean booleanField, byte[] byteArrayField,
            ElementType elementTypeField, LabeledPoint labeledPoint, HashMap<String, String> goodMap) {
        super();
        this.longField = longField;
        this.stringField = stringField;
        this.booleanField = booleanField;
        this.byteArrayField = byteArrayField;
        this.elementTypeField = elementTypeField;
        this.labeledPoint = labeledPoint;
        this.goodMap = goodMap;
    }
}