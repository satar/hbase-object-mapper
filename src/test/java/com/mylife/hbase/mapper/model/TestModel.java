// =======================================================
// Copyright Mylife.com Inc., 2014. All rights reserved.
//
// =======================================================

package com.mylife.hbase.mapper.model;

import java.util.Arrays;

import javax.mail.internet.ContentType;

import com.mylife.hbase.mapper.annotation.HBaseField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;

/**
 * A test POJO with all of the supported fields. As types are added this model should be updated as well.
 * 
 * @author Mike E
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
    
    @HBaseField(indexable = true)
    private Long indexableLongField;

    @HBaseField(indexable = true)
    private String indexableStringField;
    
    @HBaseField(indexable = true)
    private Boolean indexableBooleanField;

    @HBaseField
    private ContentType contentTypeField;

    public Integer getIntegerField() {
        return integerField;
    }

    public void setIntegerField(Integer integerField) {
        this.integerField = integerField;
    }

    public Short getShortField() {
        return shortField;
    }

    public void setShortField(Short shortField) {
        this.shortField = shortField;
    }

    public Double getDoubleField() {
        return doubleField;
    }

    public void setDoubleField(Double doubleField) {
        this.doubleField = doubleField;
    }

    public Float getFloatField() {
        return floatField;
    }

    public void setFloatField(Float floatField) {
        this.floatField = floatField;
    }

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

    public ContentType getContentTypeField() {
        return contentTypeField;
    }

    public void setContentTypeField(ContentType contentTypeField) {
        this.contentTypeField = contentTypeField;
    }
    
    public Long getIndexableLongField() {
        return indexableLongField;
    }

    public void setIndexableLongField(Long indexableLongField) {
        this.indexableLongField = indexableLongField;
    }

    public String getIndexableStringField() {
        return indexableStringField;
    }

    public void setIndexableStringField(String indexableStringField) {
        this.indexableStringField = indexableStringField;
    }

    public Boolean getIndexableBooleanField() {
        return indexableBooleanField;
    }

    public void setIndexableBooleanField(Boolean indexableBooleanField) {
        this.indexableBooleanField = indexableBooleanField;
    }

    @Override
    public String toString() {
        return "TestModel [integerField=" + integerField + ", shortField=" + shortField + ", doubleField="
                + doubleField + ", floatField=" + floatField + ", longField=" + longField + ", stringField="
                + stringField + ", booleanField=" + booleanField + ", byteArrayField="
                + Arrays.toString(byteArrayField) + ", contentTypeField=" + contentTypeField + "]";
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
        result = prime * result + ((booleanField == null) ? 0 : booleanField.hashCode());
        result = prime * result + Arrays.hashCode(byteArrayField);
        result = prime * result + ((contentTypeField == null) ? 0 : contentTypeField.hashCode());
        result = prime * result + ((doubleField == null) ? 0 : doubleField.hashCode());
        result = prime * result + ((floatField == null) ? 0 : floatField.hashCode());
        result = prime * result + ((integerField == null) ? 0 : integerField.hashCode());
        result = prime * result + ((longField == null) ? 0 : longField.hashCode());
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
}