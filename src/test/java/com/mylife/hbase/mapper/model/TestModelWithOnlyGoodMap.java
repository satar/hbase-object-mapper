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

import java.util.Map;

import com.mylife.hbase.mapper.annotation.HBaseMapField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;

/**
 * A test POJO with only a good map backing object
 * 
 * @author Mike E
 */

@HBasePersistance(tableName = "TEST_MODEL", defaultColumnFamilyName = "STUFF")
public class TestModelWithOnlyGoodMap {

    @HBaseMapField(columnFamilyName = "MAP_STUFF")
    private Map<String, String> goodMap;

    public Map<String, String> getGoodMap() {
        return goodMap;
    }

    public void setGoodMap(Map<String, String> goodMap) {
        this.goodMap = goodMap;
    }

    @Override
    public String toString() {
        return "TestModelWithGoodMap [goodMap=" + goodMap + "]";
    }

    @HBaseRowKey
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((goodMap == null) ? 0 : goodMap.hashCode());
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
        TestModelWithOnlyGoodMap other = (TestModelWithOnlyGoodMap) obj;
        if (goodMap == null) {
            if (other.goodMap != null)
                return false;
        } else if (!goodMap.equals(other.goodMap))
            return false;
        return true;
    }

    public TestModelWithOnlyGoodMap(Map<String, String> goodMap) {
        super();
        this.goodMap = goodMap;
    }

}
