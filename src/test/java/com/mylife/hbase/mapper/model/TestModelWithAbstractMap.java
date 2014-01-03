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
import java.util.Map;

import javax.mail.internet.ContentType;

import com.mylife.hbase.mapper.annotation.HBaseField;
import com.mylife.hbase.mapper.annotation.HBaseMapField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;

/**
 * A test POJO with unsupported map value type with @HBaseMapField
 * 
 * @author Mike E
 */

@HBasePersistance(tableName = "TEST_MODEL", defaultColumnFamilyName = "STUFF")
public class TestModelWithAbstractMap {

    @HBaseField
    private long longField;

    @HBaseField
    private String stringField;

    @HBaseField
    private boolean booleanField;

    @HBaseField
    private byte[] byteArrayField;
    
    @HBaseField
    private ContentType contentTypeField;
    
    @HBaseMapField
    private BadMap<String, String> badMap;

    public long getLongField() {
        return longField;
    }

    public void setLongField(long longField) {
        this.longField = longField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    public boolean getBooleanField() {
        return booleanField;
    }

    public void setBooleanField(boolean booleanField) {
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

    public BadMap<String, String> getBadMap() {
        return badMap;
    }

    public void setBadMap(BadMap<String, String> badMap) {
        this.badMap = badMap;
    }

    @HBaseRowKey
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((badMap == null) ? 0 : badMap.hashCode());
        result = prime * result + (booleanField ? 1231 : 1237);
        result = prime * result + Arrays.hashCode(byteArrayField);
        result = prime * result + ((contentTypeField == null) ? 0 : contentTypeField.hashCode());
        result = prime * result + (int) (longField ^ (longField >>> 32));
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
        TestModelWithAbstractMap other = (TestModelWithAbstractMap) obj;
        if (badMap == null) {
            if (other.badMap != null)
                return false;
        } else if (!badMap.equals(other.badMap))
            return false;
        if (booleanField != other.booleanField)
            return false;
        if (!Arrays.equals(byteArrayField, other.byteArrayField))
            return false;
        if (contentTypeField == null) {
            if (other.contentTypeField != null)
                return false;
        } else if (!contentTypeField.equals(other.contentTypeField))
            return false;
        if (longField != other.longField)
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
        builder.append("TestModelWithGoodMap [longField=").append(longField).append(", stringField=")
                .append(stringField).append(", booleanField=").append(booleanField).append(", byteArrayField=")
                .append(Arrays.toString(byteArrayField)).append(", contentTypeField=").append(contentTypeField)
                .append(", badMap=").append(badMap).append("]");
        return builder.toString();
    }

    public TestModelWithAbstractMap(long longField, String stringField, boolean booleanField, byte[] byteArrayField,
            ContentType contentTypeField, BadMap<String, String> badMap) {
        super();
        this.longField = longField;
        this.stringField = stringField;
        this.booleanField = booleanField;
        this.byteArrayField = byteArrayField;
        this.contentTypeField = contentTypeField;
        this.badMap = badMap;
    }

    private abstract class BadMap<K,V> implements Map<K, V> {
        
    }
    
}