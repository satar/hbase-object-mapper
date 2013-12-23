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

import javax.mail.internet.ContentType;

import com.mylife.hbase.mapper.annotation.HBaseField;
import com.mylife.hbase.mapper.annotation.HBaseMapField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;

/**
 * A test POJO with some valid types and a good map backing object
 * 
 * @author
 */

@HBasePersistance(tableName = "TEST_MODEL", defaultColumnFamilyName = "STUFF")
public class TestModelWithNoMap {

    @HBaseField
    private Integer integerField;

    @HBaseField
    private String stringField;

    @HBaseField
    private Boolean booleanField;

    @HBaseField
    private byte[] byteArrayField;
    
    @HBaseField
    private ContentType contentTypeField;
    
    @HBaseMapField
    private List<Long> list;

    public Integer getIntegerField() {
        return integerField;
    }

    public void setIntegerField(Integer integerField) {
        this.integerField = integerField;
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

    public ContentType getContentTypeField() {
        return contentTypeField;
    }

    public void setContentTypeField(ContentType contentTypeField) {
        this.contentTypeField = contentTypeField;
    }

    public List<Long> getList() {
        return list;
    }

    public void setList(List<Long> list) {
        this.list = list;
    }

    @Override
    @HBaseRowKey
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((booleanField == null) ? 0 : booleanField.hashCode());
        result = prime * result + Arrays.hashCode(byteArrayField);
        result = prime * result + ((contentTypeField == null) ? 0 : contentTypeField.hashCode());
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        result = prime * result + ((integerField == null) ? 0 : integerField.hashCode());
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
        TestModelWithNoMap other = (TestModelWithNoMap) obj;
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
        } else if (!contentTypeField.equals(other.contentTypeField))
            return false;
        if (list == null) {
            if (other.list != null)
                return false;
        } else if (!list.equals(other.list))
            return false;
        if (integerField == null) {
            if (other.integerField != null)
                return false;
        } else if (!integerField.equals(other.integerField))
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
        StringBuilder builder = new StringBuilder();
        builder.append("TestModelWithGoodMap [longField=").append(integerField).append(", stringField=")
                .append(stringField).append(", booleanField=").append(booleanField).append(", byteArrayField=")
                .append(Arrays.toString(byteArrayField)).append(", contentTypeField=").append(contentTypeField)
                .append(", badMap=").append(list).append("]");
        return builder.toString();
    }

    public TestModelWithNoMap(Integer integerField, String stringField, Boolean booleanField, byte[] byteArrayField,
            ContentType contentTypeField, List<Long> list) {
        super();
        this.integerField = integerField;
        this.stringField = stringField;
        this.booleanField = booleanField;
        this.byteArrayField = byteArrayField;
        this.contentTypeField = contentTypeField;
        this.list = list;
    }


    
}
