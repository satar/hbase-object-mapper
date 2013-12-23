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

package com.mylife.hbase.mapper;

import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import javax.mail.internet.ContentType;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.model.TestModel;
import com.mylife.hbase.mapper.model.TestModelOnlyFields;
import com.mylife.hbase.mapper.model.TestModelWithBadMap;
import com.mylife.hbase.mapper.model.TestModelWithDifferentBadMap;
import com.mylife.hbase.mapper.model.TestModelWithGoodMap;
import com.mylife.hbase.mapper.model.TestModelWithNoMap;
import com.mylife.hbase.mapper.util.TypeHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Description here!
 * 
 * 
 * @author Mike E
 */
@RunWith(PowerMockRunner.class)
public class HBaseEntityMapperUnitTest {

    private final TestModel testModelExpected = new TestModel(1l, "2", false, new byte[] { 3 },
            (ContentType) Whitebox.getInternalState(TypeHandler.class, "DEFAULT_CONTENT_TYPE"));
    private final TestModel testModelNullContentTypeExpected = new TestModel(1l, "2", false, new byte[] { 3 }, null);

    private final TestModelWithGoodMap testModelWithGoodMapExpected = new TestModelWithGoodMap(1l, "2", false,
            new byte[] { 3 }, ElementType.ANNOTATION_TYPE, ImmutableMap.of("testKey", "testValue", "otherKey",
                    "otherValue"));

    @SuppressWarnings("rawtypes")
    private final static Map<Class, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodExpected = BUILD_TEST_MAP();

    private static Map<Class, ImmutableMap<Field, Method>> BUILD_TEST_MAP() {

        Map<Class, ImmutableMap<Field, Method>> testMap = new HashMap<Class, ImmutableMap<Field, Method>>();

        testMap.put(
                (Class) TestModel.class,
                ImmutableMap.of(Whitebox.getField(TestModel.class, "longField"),
                        Whitebox.getMethod(TestModel.class, "getLongField"),

                        Whitebox.getField(TestModel.class, "stringField"),
                        Whitebox.getMethod(TestModel.class, "getStringField"),

                        Whitebox.getField(TestModel.class, "booleanField"),
                        Whitebox.getMethod(TestModel.class, "getBooleanField"),

                        Whitebox.getField(TestModel.class, "byteArrayField"),
                        Whitebox.getMethod(TestModel.class, "getByteArrayField"),

                        Whitebox.getField(TestModel.class, "contentTypeField"),
                        Whitebox.getMethod(TestModel.class, "getContentTypeField")));
        testMap.put(
                (Class) TestModelWithGoodMap.class,
                ImmutableMap.of(Whitebox.getField(TestModelWithGoodMap.class, "longField"),
                        Whitebox.getMethod(TestModelWithGoodMap.class, "getLongField"),

                        Whitebox.getField(TestModelWithGoodMap.class, "stringField"),
                        Whitebox.getMethod(TestModelWithGoodMap.class, "getStringField"),

                        Whitebox.getField(TestModelWithGoodMap.class, "booleanField"),
                        Whitebox.getMethod(TestModelWithGoodMap.class, "getBooleanField"),

                        Whitebox.getField(TestModelWithGoodMap.class, "byteArrayField"),
                        Whitebox.getMethod(TestModelWithGoodMap.class, "getByteArrayField"),

                        Whitebox.getField(TestModelWithGoodMap.class, "elementTypeField"),
                        Whitebox.getMethod(TestModelWithGoodMap.class, "getElementTypeField")));
        testMap.put(
                (Class) TestModelWithBadMap.class,
                ImmutableMap.of(Whitebox.getField(TestModelWithBadMap.class, "longField"),
                        Whitebox.getMethod(TestModelWithBadMap.class, "getLongField"),

                        Whitebox.getField(TestModelWithBadMap.class, "stringField"),
                        Whitebox.getMethod(TestModelWithBadMap.class, "getStringField"),

                        Whitebox.getField(TestModelWithBadMap.class, "booleanField"),
                        Whitebox.getMethod(TestModelWithBadMap.class, "getBooleanField"),

                        Whitebox.getField(TestModelWithBadMap.class, "byteArrayField"),
                        Whitebox.getMethod(TestModelWithBadMap.class, "getByteArrayField"),

                        Whitebox.getField(TestModelWithBadMap.class, "contentTypeField"),
                        Whitebox.getMethod(TestModelWithBadMap.class, "getContentTypeField")));
        testMap.put(
                (Class) TestModelWithDifferentBadMap.class,
                ImmutableMap.of(Whitebox.getField(TestModelWithDifferentBadMap.class, "longField"),
                        Whitebox.getMethod(TestModelWithDifferentBadMap.class, "getLongField"),

                        Whitebox.getField(TestModelWithDifferentBadMap.class, "stringField"),
                        Whitebox.getMethod(TestModelWithDifferentBadMap.class, "getStringField"),

                        Whitebox.getField(TestModelWithDifferentBadMap.class, "booleanField"),
                        Whitebox.getMethod(TestModelWithDifferentBadMap.class, "getBooleanField"),

                        Whitebox.getField(TestModelWithDifferentBadMap.class, "byteArrayField"),
                        Whitebox.getMethod(TestModelWithDifferentBadMap.class, "getByteArrayField"),

                        Whitebox.getField(TestModelWithDifferentBadMap.class, "contentTypeField"),
                        Whitebox.getMethod(TestModelWithDifferentBadMap.class, "getContentTypeField")));
        testMap.put(
                (Class) TestModelWithNoMap.class,
                ImmutableMap.of(Whitebox.getField(TestModelWithNoMap.class, "integerField"),
                        Whitebox.getMethod(TestModelWithNoMap.class, "getIntegerField"),

                        Whitebox.getField(TestModelWithNoMap.class, "stringField"),
                        Whitebox.getMethod(TestModelWithNoMap.class, "getStringField"),

                        Whitebox.getField(TestModelWithNoMap.class, "booleanField"),
                        Whitebox.getMethod(TestModelWithNoMap.class, "getBooleanField"),

                        Whitebox.getField(TestModelWithNoMap.class, "byteArrayField"),
                        Whitebox.getMethod(TestModelWithNoMap.class, "getByteArrayField"),

                        Whitebox.getField(TestModelWithNoMap.class, "contentTypeField"),
                        Whitebox.getMethod(TestModelWithNoMap.class, "getContentTypeField")));
        testMap.put(
                (Class) TestModelOnlyFields.class,
                ImmutableMap.of(Whitebox.getField(TestModelOnlyFields.class, "longField"),
                        Whitebox.getMethod(TestModelOnlyFields.class, "getLongField"),

                        Whitebox.getField(TestModelOnlyFields.class, "stringField"),
                        Whitebox.getMethod(TestModelOnlyFields.class, "getStringField"),

                        Whitebox.getField(TestModelOnlyFields.class, "booleanField"),
                        Whitebox.getMethod(TestModelOnlyFields.class, "getBooleanField"),

                        Whitebox.getField(TestModelOnlyFields.class, "byteArrayField"),
                        Whitebox.getMethod(TestModelOnlyFields.class, "getByteArrayField"),

                        Whitebox.getField(TestModelOnlyFields.class, "elementTypeField"),
                        Whitebox.getMethod(TestModelOnlyFields.class, "getElementTypeField")));
        return testMap;
    }

    @Mock
    HTablePool hTablePool;

    @Mock
    HTableInterface hTableInterface;

    @Mock
    Result result;

    HBaseEntityMapper hBaseEntityMapper;

    @SuppressWarnings("rawtypes")
    @Before
    @Test
    public void hBaseEntityMapperConstrutorTest() throws Exception {
        this.hBaseEntityMapper = new HBaseEntityMapper(hTablePool, "com.mylife.hbase.mapper.model");
        // This setups the state of the entity mapper lets inspect that too make
        // sure its right.
        ImmutableMap<Class, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod");
        assertFalse(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual.isEmpty());
        assertEquals(6, annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual.size());
        assertNotNull(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual
                .get((Class) TestModel.class));
        assertEquals(5,
                annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual.get((Class) TestModel.class)
                        .size());
        assertEquals(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodExpected,
                annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual);

        ImmutableMap<Class, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod");
        assertFalse(annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.isEmpty());
        assertEquals(1, annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.size());
        assertNotNull(annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual
                .get((Class) TestModelWithGoodMap.class));
        assertEquals(
                1,
                annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.get(
                        (Class) TestModelWithGoodMap.class).size());
    }

    @Test
    public void saveTest() throws Exception {

        when(hTablePool.getTable(TestModel.class.getAnnotation(HBasePersistance.class).tableName())).thenReturn(
                hTableInterface);
        this.hBaseEntityMapper.save(testModelExpected);
    }

    @Test
    public void saveTestNullContentType() throws Exception {

        when(hTablePool.getTable(TestModel.class.getAnnotation(HBasePersistance.class).tableName())).thenReturn(
                hTableInterface);
        this.hBaseEntityMapper.save(testModelNullContentTypeExpected);
    }

    @Test
    public void saveMapTest() throws Exception {

        when(hTablePool.getTable(TestModel.class.getAnnotation(HBasePersistance.class).tableName())).thenReturn(
                hTableInterface);
        this.hBaseEntityMapper.save(testModelWithGoodMapExpected);
    }

    @Test
    public void objectFromTest() throws Exception {
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilyResultMap = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(
                Bytes.BYTES_COMPARATOR);
        columnFamilyResultMap.put(
                Bytes.toBytes("OTHER_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("longField"), Bytes.toBytes(testModelExpected.getLongField())).build()));
        columnFamilyResultMap.put(
                Bytes.toBytes("STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("stringField"), Bytes.toBytes(testModelExpected.getStringField())).build()));
        columnFamilyResultMap.put(
                Bytes.toBytes("MORE_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("booleanField"), Bytes.toBytes(testModelExpected.getBooleanField())).build()));
        columnFamilyResultMap.get(Bytes.toBytes("OTHER_STUFF")).put(Bytes.toBytes("byteArrayField"),
                testModelExpected.getByteArrayField());
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("contentTypeField"),
                Bytes.toBytes(testModelExpected.getContentTypeField().toString()));

        when(result.getNoVersionMap()).thenReturn(columnFamilyResultMap);

        assertEquals(testModelExpected, this.hBaseEntityMapper.objectFrom(result, TestModel.class));
    }

    @Test
    public void objectFromNoResultsTest() throws Exception {

        when(result.isEmpty()).thenReturn(true);

        assertNull(this.hBaseEntityMapper.objectFrom(result, TestModel.class));
    }

    @Test
    public void objectFromNullContentTypeTest() throws Exception {

        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilyResultMap = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(
                Bytes.BYTES_COMPARATOR);
        columnFamilyResultMap.put(
                Bytes.toBytes("OTHER_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("longField"), Bytes.toBytes(testModelNullContentTypeExpected.getLongField()))
                        .build()));
        columnFamilyResultMap.put(
                Bytes.toBytes("STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("stringField"), Bytes.toBytes(testModelNullContentTypeExpected.getStringField()))
                        .build()));
        columnFamilyResultMap.put(
                Bytes.toBytes("MORE_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("booleanField"),
                        Bytes.toBytes(testModelNullContentTypeExpected.getBooleanField())).build()));
        columnFamilyResultMap.get(Bytes.toBytes("OTHER_STUFF")).put(Bytes.toBytes("byteArrayField"),
                testModelNullContentTypeExpected.getByteArrayField());
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("contentTypeField"), null);

        when(result.getNoVersionMap()).thenReturn(columnFamilyResultMap);

        assertEquals(testModelNullContentTypeExpected, this.hBaseEntityMapper.objectFrom(result, TestModel.class));

    }

    @Test
    public void objectFromMapTest() throws Exception {
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilyResultMap = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(
                Bytes.BYTES_COMPARATOR);
        
        columnFamilyResultMap.put(
                Bytes.toBytes("OTHER_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("longField"), Bytes.toBytes(testModelWithGoodMapExpected.getLongField())).build()));
        columnFamilyResultMap.put(
                Bytes.toBytes("STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("stringField"), Bytes.toBytes(testModelWithGoodMapExpected.getStringField())).build()));
        columnFamilyResultMap.put(
                Bytes.toBytes("MORE_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("booleanField"), Bytes.toBytes(testModelWithGoodMapExpected.getBooleanField())).build()));
        columnFamilyResultMap.get(Bytes.toBytes("OTHER_STUFF")).put(Bytes.toBytes("byteArrayField"),
                testModelWithGoodMapExpected.getByteArrayField());

        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(
                        Bytes.toBytes("elementTypeField"), Bytes.toBytes(ElementType.ANNOTATION_TYPE.name()));
        columnFamilyResultMap.put(Bytes.toBytes("MAP_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(Bytes.toBytes("testKey"), Bytes.toBytes("testValue")).build()));
        columnFamilyResultMap.get(Bytes.toBytes("MAP_STUFF")).put(Bytes.toBytes("otherKey"), Bytes.toBytes("otherValue"));

        when(result.getNoVersionMap()).thenReturn(columnFamilyResultMap);

        assertEquals(testModelWithGoodMapExpected,
                this.hBaseEntityMapper.objectFrom(result, TestModelWithGoodMap.class));
    }
}
