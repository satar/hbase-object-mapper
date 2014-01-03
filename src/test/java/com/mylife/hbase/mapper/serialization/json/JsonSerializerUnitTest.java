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

package com.mylife.hbase.mapper.serialization.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.mylife.hbase.mapper.model.LabeledPoint;
import com.mylife.hbase.mapper.model.LabeledPointsWrapper;

/**
 * unit test for the KryoSerialization
 * 
 * 
 * @author Mike E
 */

public class JsonSerializerUnitTest {

    private final LabeledPoint labeledPointExcepted = new LabeledPoint("Label", -1, 1);
    private final LabeledPointsWrapper labeledPointsWrapper = new LabeledPointsWrapper( new ArrayList<LabeledPoint>(
            ImmutableList.of(labeledPointExcepted)));

    @Test
    public void testNullSerialization() throws Exception {
        assertNull(JsonSerializer.newInstance().serialize(null));
    }

    @Test
    public void testNullDeserialization() throws Exception {
        assertNull(JsonSerializer.newInstance().deserialize(null, LabeledPoint.class));
    }

    @Test
    public void testSerialzationDeserialiationLifeCycle() throws Exception {
        JsonSerializer jsonSerializer = JsonSerializer.newInstance();
        assertEquals(labeledPointExcepted,
                jsonSerializer.deserialize(jsonSerializer.serialize(labeledPointExcepted), LabeledPoint.class));
    }

    @Test
    public void testSerialzationDeserialiationLifeCycleWithArrayList() throws Exception {
        JsonSerializer jsonSerializer = JsonSerializer.newInstance();
        assertEquals(labeledPointsWrapper,
                jsonSerializer.deserialize(jsonSerializer.serialize(labeledPointsWrapper), LabeledPointsWrapper.class));
    }

}
