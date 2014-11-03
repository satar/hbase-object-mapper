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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.powermock.reflect.Whitebox;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.mylife.hbase.mapper.model.LabeledPoint;
import com.mylife.hbase.mapper.model.LabeledPointsWrapper;
import com.mylife.hbase.mapper.serialization.HBaseObjectSerializer;

public class AbstractSerializationUnitTest {
    private final LabeledPoint labeledPointExcepted = new LabeledPoint("Label", -1, 1);
    private final List<LabeledPoint> labeledPointsExceptedArrayList = new ArrayList<LabeledPoint>(
            ImmutableList.of(labeledPointExcepted));
    private final List<LabeledPoint> labeledPointsExceptedLinkedList = new LinkedList<LabeledPoint>(
            ImmutableList.of(labeledPointExcepted));
    private final Map<Long, LabeledPoint> labeledPointsExceptedMap = new HashMap<Long, LabeledPoint>(ImmutableMap.of(
            1234L, labeledPointExcepted));
    private final Set<LabeledPoint> labeledPointsExceptedSet = new HashSet<LabeledPoint>(
            ImmutableSet.of(labeledPointExcepted));
    private final Collection<LabeledPoint> labeledPointsExceptedCollection = new ArrayList<LabeledPoint>(
            ImmutableSet.of(labeledPointExcepted));
    private final Queue<LabeledPoint> labeledPointsExceptedQueue = new LinkedList<LabeledPoint>(
            ImmutableSet.of(labeledPointExcepted));
    private final LabeledPointsWrapper labeledPointsWrapperExcepted = new LabeledPointsWrapper(
            labeledPointsExceptedArrayList);

    protected void testNullSerialization(final HBaseObjectSerializer baseObjectSerializer) throws Exception {
        assertNull(baseObjectSerializer.serialize(null));
    }

    protected void testNullDeserialization(final HBaseObjectSerializer baseObjectSerializer) throws Exception {
        assertNull(baseObjectSerializer.deserialize(null,
                Whitebox.getField(AbstractSerializationUnitTest.class, "labeledPointExcepted")));
    }

    protected void testSerialzationDeserialiationLifeCycle(final HBaseObjectSerializer baseObjectSerializer)
            throws Exception {
        assertEquals(
                labeledPointExcepted,
                baseObjectSerializer.deserialize(baseObjectSerializer.serialize(labeledPointExcepted),
                        Whitebox.getField(AbstractSerializationUnitTest.class, "labeledPointExcepted")));
    }

    protected void testSerialzationDeserialiationLifeCycleWithList(final HBaseObjectSerializer baseObjectSerializer)
            throws Exception {
        assertEquals(labeledPointsExceptedArrayList, baseObjectSerializer.deserialize(
                baseObjectSerializer.serialize(labeledPointsExceptedArrayList),
                Whitebox.getField(AbstractSerializationUnitTest.class, "labeledPointsExceptedArrayList")));
    }

    protected void testSerialzationDeserialiationLifeCycleWithLinkedList(
            final HBaseObjectSerializer baseObjectSerializer) throws Exception {
        assertEquals(labeledPointsExceptedLinkedList, baseObjectSerializer.deserialize(
                baseObjectSerializer.serialize(labeledPointsExceptedLinkedList),
                Whitebox.getField(AbstractSerializationUnitTest.class, "labeledPointsExceptedLinkedList")));
    }

    protected void testSerialzationDeserialiationLifeCycleWithMap(final HBaseObjectSerializer baseObjectSerializer)
            throws Exception {
        assertEquals(
                labeledPointsExceptedMap,
                baseObjectSerializer.deserialize(baseObjectSerializer.serialize(labeledPointsExceptedMap),
                        Whitebox.getField(AbstractSerializationUnitTest.class, "labeledPointsExceptedMap")));
    }

    protected void testSerialzationDeserialiationLifeCycleWithSet(final HBaseObjectSerializer baseObjectSerializer)
            throws Exception {
        assertEquals(
                labeledPointsExceptedSet,
                baseObjectSerializer.deserialize(baseObjectSerializer.serialize(labeledPointsExceptedSet),
                        Whitebox.getField(AbstractSerializationUnitTest.class, "labeledPointsExceptedSet")));
    }

    protected void testSerialzationDeserialiationLifeCycleWithCollection(
            final HBaseObjectSerializer baseObjectSerializer) throws Exception {
        assertEquals(labeledPointsExceptedCollection, baseObjectSerializer.deserialize(
                baseObjectSerializer.serialize(labeledPointsExceptedCollection),
                Whitebox.getField(AbstractSerializationUnitTest.class, "labeledPointsExceptedCollection")));
    }

    protected void testSerialzationDeserialiationLifeCycleWithQueue(final HBaseObjectSerializer baseObjectSerializer)
            throws Exception {
        assertEquals(
                labeledPointsExceptedQueue,
                baseObjectSerializer.deserialize(baseObjectSerializer.serialize(labeledPointsExceptedQueue),
                        Whitebox.getField(AbstractSerializationUnitTest.class, "labeledPointsExceptedQueue")));
    }

    protected void testSerialzationDeserialiationLifeCycleWithWrappedArrayList(
            final HBaseObjectSerializer baseObjectSerializer) throws Exception {
        assertEquals(
                labeledPointsWrapperExcepted,
                baseObjectSerializer.deserialize(baseObjectSerializer.serialize(labeledPointsWrapperExcepted),
                        Whitebox.getField(AbstractSerializationUnitTest.class, "labeledPointsWrapperExcepted")));
    }

}