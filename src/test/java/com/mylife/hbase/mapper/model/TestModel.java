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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.mail.internet.ContentType;

import com.mylife.hbase.mapper.annotation.HBaseField;
import com.mylife.hbase.mapper.annotation.HBaseMapField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;

/**
 * A test POJO with all of the supported types. As types are added this model
 * should be updated as well.
 * 
 * @author
 */

@HBasePersistance(tableName = "TEST_MODEL", defaultColumnFamilyName = "STUFF")
public class TestModel {

    @HBaseField
    private Integer integerField;

    @HBaseField
    private Short shortField;

    @HBaseField
    private Double doubleField;

    @HBaseField
    private Float floatField;

    @HBaseField(columnFamilyName = "OTHER_STUFF")
    private Long longField;

    @HBaseField
    private String stringField;

    @HBaseField(columnFamilyName = "MORE_STUFF")
    private Boolean booleanField;

    @HBaseField(columnFamilyName = "OTHER_STUFF")
    private byte[] byteArrayField;

    @HBaseField
    private ContentType contentTypeField;

    @HBaseMapField
    private Map<Long, Object> badMap;

    @HBaseMapField
    private List<Long> notAMap;

    public Integer getIntegerField() {
        return integerField;
    }

    public Short getShortField() {
        return shortField;
    }

    public Double getDoubleField() {
        return doubleField;
    }

    public Float getFloatField() {
        return floatField;
    }

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

    public ContentType getContentTypeField() {
        return contentTypeField;
    }

    public Map<Long, Object> getBadMap() {
        return badMap;
    }

    public List<Long> getNotAMap() {
        return notAMap;
    }

    public TestModel(Integer integerField, Short shortField, Double doubleField, Float floatField, Long longField,
            String stringField, Boolean booleanField, byte[] byteArrayField, ContentType contentTypeField) {
        super();
        this.integerField = integerField;
        this.shortField = shortField;
        this.doubleField = doubleField;
        this.floatField = floatField;
        this.longField = longField;
        this.stringField = stringField;
        this.booleanField = booleanField;
        this.byteArrayField = byteArrayField;
        this.contentTypeField = contentTypeField;
    }

    @HBaseRowKey
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((badMap == null) ? 0 : badMap.hashCode());
        result = prime * result + ((booleanField == null) ? 0 : booleanField.hashCode());
        result = prime * result + Arrays.hashCode(byteArrayField);
        result = prime * result + ((contentTypeField == null) ? 0 : contentTypeField.hashCode());
        result = prime * result + ((doubleField == null) ? 0 : doubleField.hashCode());
        result = prime * result + ((floatField == null) ? 0 : floatField.hashCode());
        result = prime * result + ((integerField == null) ? 0 : integerField.hashCode());
        result = prime * result + ((longField == null) ? 0 : longField.hashCode());
        result = prime * result + ((notAMap == null) ? 0 : notAMap.hashCode());
        result = prime * result + ((shortField == null) ? 0 : shortField.hashCode());
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
        TestModel other = (TestModel) obj;
        if (badMap == null) {
            if (other.badMap != null)
                return false;
        } else if (!badMap.equals(other.badMap))
            return false;
        if (booleanField == null) {
            if (other.booleanField != null)
                return false;
        } else if (!booleanField.equals(other.booleanField))
            return false;
        if (!Arrays.equals(byteArrayField, other.byteArrayField))
            return false;
        if (contentTypeField == null) {
            if (other.contentTypeField != null)
                return false;
        } else if (!contentTypeField.toString().equals(other.contentTypeField.toString()))
            return false;
        if (doubleField == null) {
            if (other.doubleField != null)
                return false;
        } else if (!doubleField.equals(other.doubleField))
            return false;
        if (floatField == null) {
            if (other.floatField != null)
                return false;
        } else if (!floatField.equals(other.floatField))
            return false;
        if (integerField == null) {
            if (other.integerField != null)
                return false;
        } else if (!integerField.equals(other.integerField))
            return false;
        if (longField == null) {
            if (other.longField != null)
                return false;
        } else if (!longField.equals(other.longField))
            return false;
        if (notAMap == null) {
            if (other.notAMap != null)
                return false;
        } else if (!notAMap.equals(other.notAMap))
            return false;
        if (shortField == null) {
            if (other.shortField != null)
                return false;
        } else if (!shortField.equals(other.shortField))
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
        return "TestModel [integerField=" + integerField + ", shortField=" + shortField + ", doubleField="
                + doubleField + ", floatField=" + floatField + ", longField=" + longField + ", stringField="
                + stringField + ", booleanField=" + booleanField + ", byteArrayField="
                + Arrays.toString(byteArrayField) + ", contentTypeField=" + contentTypeField + ", badMap=" + badMap
                + ", notAMap=" + notAMap + "]";
    }

}
