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

import com.mylife.hbase.mapper.SerializationStategy;
import com.mylife.hbase.mapper.annotation.HBaseObjectField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;

/**
 * A test POJO with only a {@link @HBaseObjectField} annotated field
 * 
 * @author Mike E
 */

@HBasePersistance(tableName = "TEST_MODEL", defaultColumnFamilyName = "STUFF")
public class TestModelWithOnlyObjectFields {

    @HBaseObjectField(serializationStategy=SerializationStategy.JSON)
    private LabeledPoint labeledPoint;

    public LabeledPoint getLabeledPoint() {
        return labeledPoint;
    }

    public void setLabeledPoint(LabeledPoint labeledPoint) {
        this.labeledPoint = labeledPoint;
    }

    @Override
    public String toString() {
        return "TestModelWithOnlyObjectFields [labeledPoint=" + labeledPoint + "]";
    }

    @HBaseRowKey
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((labeledPoint == null) ? 0 : labeledPoint.hashCode());
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
        TestModelWithOnlyObjectFields other = (TestModelWithOnlyObjectFields) obj;
        if (labeledPoint == null) {
            if (other.labeledPoint != null)
                return false;
        } else if (!labeledPoint.equals(other.labeledPoint))
            return false;
        return true;
    }

    public TestModelWithOnlyObjectFields(LabeledPoint labeledPoint) {
        super();
        this.labeledPoint = labeledPoint;
    }

}