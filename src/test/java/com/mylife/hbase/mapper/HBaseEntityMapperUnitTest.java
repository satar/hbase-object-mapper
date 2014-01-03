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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import javax.mail.internet.ContentType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.iq80.snappy.SnappyOutputStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.mockito.mockpolicies.Slf4jMockPolicy;
import org.powermock.core.classloader.annotations.MockPolicy;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSortedMap;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.model.LabeledPoint;
import com.mylife.hbase.mapper.model.TestModel;
import com.mylife.hbase.mapper.model.TestModelWithGoodHashMap;
import com.mylife.hbase.mapper.model.TestModelWithOnlyGoodMap;
import com.mylife.hbase.mapper.model.TestModelWithOnlyObjectFields;
import com.mylife.hbase.mapper.model.TestModelWithUnsupportedTypeAnnotated;
import com.mylife.hbase.mapper.util.TypeHandler;

/**
 * Unit testing for the core class HBaseEntityMapper
 * 
 * 
 * @author Mike E
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("org.apache.log4j.*")
@MockPolicy(Slf4jMockPolicy.class)
@PrepareForTest({ HBaseEntityMapper.class, HBaseAdmin.class })
public class HBaseEntityMapperUnitTest {

    private final TestModel testModelExpected = new TestModel(
            -1,
            (short) 0,
            2.71828182845904523536028747135266249775724709369995,
            3.14159265358979323846264338327950288419716939937510582097494459230781640628620899862803482534211706798214808651328230664709384460955058223172535940812848111745028410270193852110555964462294895493038196442881097566593344612847564823378678316527120190914564856692346F,
            4L, "5", false, new byte[] { 6 }, (ContentType) Whitebox.getInternalState(TypeHandler.class,
                    "DEFAULT_CONTENT_TYPE"));
    private final TestModel testModelNullContentTypeExpected = new TestModel(
            -1,
            (short) 0,
            2.71828182845904523536028747135266249775724709369995,
            3.14159265358979323846264338327950288419716939937510582097494459230781640628620899862803482534211706798214808651328230664709384460955058223172535940812848111745028410270193852110555964462294895493038196442881097566593344612847564823378678316527120190914564856692346F,
            4L, "5", false, new byte[] { 6 }, null);

    private final TestModelWithOnlyGoodMap testModelWithOnlyGoodMap = new TestModelWithOnlyGoodMap(ImmutableMap.of(
            "testKey", "testValue", "otherKey", "otherValue"));

    private final LabeledPoint labledPoint = new LabeledPoint("labeled", -1, 1);

    private final TestModelWithGoodHashMap testModelWithGoodHashMapExpected = new TestModelWithGoodHashMap(1l, "2",
            false, new byte[] { 3 }, ElementType.ANNOTATION_TYPE, labledPoint, new HashMap<String, String>(
                    ImmutableMap.of("testKey", "testValue", "otherKey", "otherValue")));

    private final TestModelWithUnsupportedTypeAnnotated testModelWithUnsupportedTypeAnnotated = new TestModelWithUnsupportedTypeAnnotated(
            (short) 0, ImmutableList.of((short) 1, (short) 2));

    private final TestModelWithOnlyGoodMap testModelWithOnlyGoodMapExpected = new TestModelWithOnlyGoodMap(
            ImmutableMap.of("", "Hi!"));

    private final TestModelWithOnlyObjectFields testModelWithOnlyObjectFieldsExpected = new TestModelWithOnlyObjectFields(
            labledPoint);

    private final static Map<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodExpected = BUILD_TEST_MAP();

    private static Map<Class<?>, ImmutableMap<Field, Method>> BUILD_TEST_MAP() {

        Map<Class<?>, ImmutableMap<Field, Method>> testMap = new HashMap<Class<?>, ImmutableMap<Field, Method>>();

        Builder<Field, Method> builder = ImmutableMap.builder();

        // empty maps
        testMap.put((Class<?>) TestModelWithOnlyGoodMap.class, builder.build());
        testMap.put((Class<?>) TestModelWithOnlyObjectFields.class, builder.build());

        builder.put(Whitebox.getField(TestModel.class, "longField"),
                Whitebox.getMethod(TestModel.class, "getLongField"));
        builder.put(Whitebox.getField(TestModel.class, "stringField"),
                Whitebox.getMethod(TestModel.class, "getStringField"));
        builder.put(Whitebox.getField(TestModel.class, "integerField"),
                Whitebox.getMethod(TestModel.class, "getIntegerField"));
        builder.put(Whitebox.getField(TestModel.class, "doubleField"),
                Whitebox.getMethod(TestModel.class, "getDoubleField"));
        builder.put(Whitebox.getField(TestModel.class, "floatField"),
                Whitebox.getMethod(TestModel.class, "getFloatField"));
        builder.put(Whitebox.getField(TestModel.class, "shortField"),
                Whitebox.getMethod(TestModel.class, "getShortField"));
        builder.put(Whitebox.getField(TestModel.class, "booleanField"),
                Whitebox.getMethod(TestModel.class, "getBooleanField"));
        builder.put(Whitebox.getField(TestModel.class, "byteArrayField"),
                Whitebox.getMethod(TestModel.class, "getByteArrayField"));
        builder.put(Whitebox.getField(TestModel.class, "contentTypeField"),
                Whitebox.getMethod(TestModel.class, "getContentTypeField"));

        testMap.put((Class<?>) TestModel.class, builder.build());

        testMap.put(
                (Class<?>) TestModelWithGoodHashMap.class,
                ImmutableMap.of(Whitebox.getField(TestModelWithGoodHashMap.class, "longField"),
                        Whitebox.getMethod(TestModelWithGoodHashMap.class, "getLongField"),

                        Whitebox.getField(TestModelWithGoodHashMap.class, "stringField"),
                        Whitebox.getMethod(TestModelWithGoodHashMap.class, "getStringField"),

                        Whitebox.getField(TestModelWithGoodHashMap.class, "booleanField"),
                        Whitebox.getMethod(TestModelWithGoodHashMap.class, "getBooleanField"),

                        Whitebox.getField(TestModelWithGoodHashMap.class, "byteArrayField"),
                        Whitebox.getMethod(TestModelWithGoodHashMap.class, "getByteArrayField"),

                        Whitebox.getField(TestModelWithGoodHashMap.class, "elementTypeField"),
                        Whitebox.getMethod(TestModelWithGoodHashMap.class, "getElementTypeField")));

        return testMap;
    }

    @Mock
    HTablePool hTablePool;

    @Mock
    HBaseAdmin hBaseAdmin;

    @Mock
    HTableDescriptor hTableDescriptor;

    @Mock
    HTableInterface hTableInterface;

    @Mock
    Result result;

    @Mock
    ResultScanner resultScanner;

    @Mock
    Iterator<Result> resultIterator;

    HBaseEntityMapper hBaseEntityMapper;

    @Before
    @Test
    public void hBaseEntityMapperConstrutorTest() throws Exception {
        final Configuration configuration = new Configuration();
        Whitebox.setInternalState(hTablePool, Configuration.class, configuration);
        PowerMockito.whenNew(HBaseAdmin.class).withArguments(configuration).thenReturn(hBaseAdmin);
        when(hBaseAdmin.tableExists(anyString())).thenReturn(true);
        when(hBaseAdmin.getTableDescriptor((byte[]) any())).thenReturn(hTableDescriptor);
        when(hTableDescriptor.hasFamily((byte[]) any())).thenReturn(true);
        this.hBaseEntityMapper = new HBaseEntityMapper(hTablePool, "com.mylife.hbase.mapper.model");
        // This setups the state of the entity mapper lets inspect that too make
        // sure that the internal state of the entity mapper is correct.
        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod");
        assertFalse(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual.isEmpty());
        assertEquals(4, annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual.size());
        assertNotNull(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual
                .get((Class<?>) TestModel.class));
        assertEquals(9,
                annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual
                        .get((Class<?>) TestModel.class).size());
        assertEquals(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodExpected,
                annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual);

        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod");
        assertFalse(annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.isEmpty());
        assertEquals(2, annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.size());
        assertNotNull(annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual
                .get((Class<?>) TestModelWithOnlyGoodMap.class));
        assertEquals(
                1,
                annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.get(
                        (Class<?>) TestModelWithOnlyGoodMap.class).size());
        assertNull(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual
                .get((Class<?>) TestModelWithUnsupportedTypeAnnotated.class));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void hBaseEntityMapperConstrutorCannotConnectToHBaseTest() throws Exception {
        final Configuration configuration = new Configuration();
        Whitebox.setInternalState(hTablePool, Configuration.class, configuration);
        PowerMockito.whenNew(HBaseAdmin.class).withArguments(configuration).thenThrow(MasterNotRunningException.class);
        this.hBaseEntityMapper = new HBaseEntityMapper(hTablePool, "com.mylife.hbase.mapper.model");
        assertNull(Whitebox.getInternalState(this.hBaseEntityMapper, "hTablePool"));
        assertNull(Whitebox.getInternalState(this.hBaseEntityMapper, "annotatedClassToAnnotatedHBaseRowKey"));
        assertNull(Whitebox.getInternalState(this.hBaseEntityMapper,
                "annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod"));
        assertNull(Whitebox.getInternalState(this.hBaseEntityMapper,
                "annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod"));
        assertNull(Whitebox.getInternalState(this.hBaseEntityMapper,
                "annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod"));
    }

    @Test
    public void hBaseEntityMapperConstrutorNoTableExistsTest() throws Exception {
        final Configuration configuration = new Configuration();
        Whitebox.setInternalState(hTablePool, Configuration.class, configuration);
        PowerMockito.whenNew(HBaseAdmin.class).withArguments(configuration).thenReturn(hBaseAdmin);
        when(hBaseAdmin.tableExists(anyString())).thenReturn(false);
        this.hBaseEntityMapper = new HBaseEntityMapper(hTablePool, "com.mylife.hbase.mapper.model");
        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod");
        assertTrue(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual.isEmpty());

        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod");
        assertTrue(annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.isEmpty());
    }

    @Test
    public void hBaseEntityMapperConstrutorCannotVerifyTableExistsTest() throws Exception {
        final Configuration configuration = new Configuration();
        Whitebox.setInternalState(hTablePool, Configuration.class, configuration);
        PowerMockito.whenNew(HBaseAdmin.class).withArguments(configuration).thenReturn(hBaseAdmin);
        when(hBaseAdmin.tableExists(anyString())).thenThrow(new IOException("TEST EXCEPTION"));
        this.hBaseEntityMapper = new HBaseEntityMapper(hTablePool, "com.mylife.hbase.mapper.model");
        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod");
        assertTrue(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual.isEmpty());

        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod");
        assertTrue(annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.isEmpty());
    }

    @Test
    public void hBaseEntityMapperConstrutorNoDefaultColumnFamilyExistsTest() throws Exception {
        final Configuration configuration = new Configuration();
        Whitebox.setInternalState(hTablePool, Configuration.class, configuration);
        PowerMockito.whenNew(HBaseAdmin.class).withArguments(configuration).thenReturn(hBaseAdmin);
        when(hBaseAdmin.tableExists(anyString())).thenReturn(true);
        when(hBaseAdmin.getTableDescriptor((byte[]) any())).thenReturn(hTableDescriptor);
        when(hTableDescriptor.hasFamily((byte[]) any())).thenReturn(false);
        this.hBaseEntityMapper = new HBaseEntityMapper(hTablePool, "com.mylife.hbase.mapper.model");
        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod");
        assertTrue(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual.isEmpty());

        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod");
        assertTrue(annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.isEmpty());
    }

    @Test
    public void hBaseEntityMapperConstrutorCannotVerifyDefaultColumnFamilyExistsTest() throws Exception {
        final Configuration configuration = new Configuration();
        Whitebox.setInternalState(hTablePool, Configuration.class, configuration);
        PowerMockito.whenNew(HBaseAdmin.class).withArguments(configuration).thenReturn(hBaseAdmin);
        when(hBaseAdmin.tableExists(anyString())).thenReturn(true);
        when(hBaseAdmin.getTableDescriptor((byte[]) any())).thenThrow(new IOException("TEST EXCEPTION"));
        this.hBaseEntityMapper = new HBaseEntityMapper(hTablePool, "com.mylife.hbase.mapper.model");
        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod");
        assertTrue(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodActual.isEmpty());

        ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual = Whitebox
                .getInternalState(hBaseEntityMapper,
                        "annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod");
        assertTrue(annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodActual.isEmpty());
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
        this.hBaseEntityMapper.save(testModelWithGoodHashMapExpected);
    }

    @Test
    public void saveOnlyMapTest() throws Exception {

        when(hTablePool.getTable(TestModel.class.getAnnotation(HBasePersistance.class).tableName())).thenReturn(
                hTableInterface);
        this.hBaseEntityMapper.save(testModelWithOnlyGoodMapExpected);
    }

    @Test
    public void saveOnlyObjectFieldTest() throws Exception {

        when(hTablePool.getTable(TestModel.class.getAnnotation(HBasePersistance.class).tableName())).thenReturn(
                hTableInterface);
        this.hBaseEntityMapper.save(testModelWithOnlyObjectFieldsExpected);
    }

    @Test(expected = IllegalArgumentException.class)
    public void saveUnsupportedTypeTest() throws Exception {

        when(hTablePool.getTable(TestModel.class.getAnnotation(HBasePersistance.class).tableName())).thenReturn(
                hTableInterface);
        this.hBaseEntityMapper.save(testModelWithUnsupportedTypeAnnotated);
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
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("integerField"),
                Bytes.toBytes(testModelExpected.getIntegerField()));
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("floatField"),
                Bytes.toBytes(testModelExpected.getFloatField()));
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("shortField"),
                Bytes.toBytes(testModelExpected.getShortField()));
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("doubleField"),
                Bytes.toBytes(testModelExpected.getDoubleField()));

        when(result.getNoVersionMap()).thenReturn(columnFamilyResultMap);

        assertEquals(testModelExpected, this.hBaseEntityMapper.objectFrom(result, TestModel.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void objectFromUnsupportedTypeAnnotatedTest() throws Exception {
        this.hBaseEntityMapper.objectFrom(result, TestModelWithUnsupportedTypeAnnotated.class);
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
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("integerField"),
                Bytes.toBytes(testModelExpected.getIntegerField()));
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("floatField"),
                Bytes.toBytes(testModelExpected.getFloatField()));
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("shortField"),
                Bytes.toBytes(testModelExpected.getShortField()));
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("doubleField"),
                Bytes.toBytes(testModelExpected.getDoubleField()));

        when(result.getNoVersionMap()).thenReturn(columnFamilyResultMap);

        assertEquals(testModelNullContentTypeExpected, this.hBaseEntityMapper.objectFrom(result, TestModel.class));

    }

    @Test
    public void objectFromMapTest() throws Exception {
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilyResultMap = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(
                Bytes.BYTES_COMPARATOR);

        columnFamilyResultMap.put(
                Bytes.toBytes("STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("testKey"), Bytes.toBytes("testValue")).build()));
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("otherKey"), Bytes.toBytes("otherValue"));

        when(result.getNoVersionMap()).thenReturn(columnFamilyResultMap);

        assertEquals(testModelWithOnlyGoodMap,
                this.hBaseEntityMapper.objectFrom(result, TestModelWithOnlyGoodMap.class));
    }

    @Test
    public void objectListFromResultScannerTest() throws Exception {
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilyResultMap = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(
                Bytes.BYTES_COMPARATOR);

        columnFamilyResultMap.put(
                Bytes.toBytes("STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("testKey"), Bytes.toBytes("testValue")).build()));
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("otherKey"), Bytes.toBytes("otherValue"));

        when(resultScanner.iterator()).thenReturn(resultIterator);
        when(resultIterator.hasNext()).thenReturn(true, false);

        when(resultIterator.next()).thenReturn(result);

        when(result.getNoVersionMap()).thenReturn(columnFamilyResultMap);

        List<TestModelWithOnlyGoodMap> testModelWithOnlyGoodMaps = this.hBaseEntityMapper.objectListFrom(resultScanner,
                TestModelWithOnlyGoodMap.class);

        assertNotNull(testModelWithOnlyGoodMaps);
        assertFalse(testModelWithOnlyGoodMaps.isEmpty());
        assertEquals(1, testModelWithOnlyGoodMaps.size());
        assertEquals(testModelWithOnlyGoodMap, testModelWithOnlyGoodMaps.get(0));
    }

    @Test
    public void objectListFromResultArrayTest() throws Exception {
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilyResultMap = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(
                Bytes.BYTES_COMPARATOR);

        columnFamilyResultMap.put(
                Bytes.toBytes("STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("testKey"), Bytes.toBytes("testValue")).build()));
        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("otherKey"), Bytes.toBytes("otherValue"));

        when(result.getNoVersionMap()).thenReturn(columnFamilyResultMap);

        List<TestModelWithOnlyGoodMap> testModelWithOnlyGoodMaps = this.hBaseEntityMapper.objectListFrom(new Result[] {
                result, result }, TestModelWithOnlyGoodMap.class);

        assertNotNull(testModelWithOnlyGoodMaps);
        assertFalse(testModelWithOnlyGoodMaps.isEmpty());
        assertEquals(2, testModelWithOnlyGoodMaps.size());
        assertEquals(testModelWithOnlyGoodMap, testModelWithOnlyGoodMaps.get(0));
    }

    @Test
    public void objectFromHashMapTest() throws Exception {
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilyResultMap = new TreeMap<byte[], NavigableMap<byte[], byte[]>>(
                Bytes.BYTES_COMPARATOR);

        columnFamilyResultMap.put(
                Bytes.toBytes("OTHER_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("longField"), Bytes.toBytes(testModelWithGoodHashMapExpected.getLongField()))
                        .build()));
        columnFamilyResultMap.put(
                Bytes.toBytes("STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("stringField"), Bytes.toBytes(testModelWithGoodHashMapExpected.getStringField()))
                        .build()));
        columnFamilyResultMap.put(
                Bytes.toBytes("MORE_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("booleanField"),
                        Bytes.toBytes(testModelWithGoodHashMapExpected.getBooleanField())).build()));
        columnFamilyResultMap.get(Bytes.toBytes("OTHER_STUFF")).put(Bytes.toBytes("byteArrayField"),
                testModelWithGoodHashMapExpected.getByteArrayField());
        columnFamilyResultMap.put(
                Bytes.toBytes("OBJECT_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("labeledPoint"), kyroOutput(testModelWithGoodHashMapExpected.getLabeledPoint()))
                        .build()));

        columnFamilyResultMap.get(Bytes.toBytes("STUFF")).put(Bytes.toBytes("elementTypeField"),
                Bytes.toBytes(ElementType.ANNOTATION_TYPE.name()));
        columnFamilyResultMap.put(
                Bytes.toBytes("MAP_STUFF"),
                new TreeMap<byte[], byte[]>(new ImmutableSortedMap.Builder<byte[], byte[]>(Bytes.BYTES_COMPARATOR).put(
                        Bytes.toBytes("testKey"), Bytes.toBytes("testValue")).build()));
        columnFamilyResultMap.get(Bytes.toBytes("MAP_STUFF")).put(Bytes.toBytes("otherKey"),
                Bytes.toBytes("otherValue"));

        when(result.getNoVersionMap()).thenReturn(columnFamilyResultMap);

        assertEquals(testModelWithGoodHashMapExpected,
                this.hBaseEntityMapper.objectFrom(result, TestModelWithGoodHashMap.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testToBytesException() throws Exception {
        Whitebox.invokeMethod(this.hBaseEntityMapper, "toBytes", labledPoint);
    }

    private byte[] kyroOutput(Object object) throws Exception {
        final Kryo kryo = new Kryo();
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final Output output = new Output(new SnappyOutputStream(byteArrayOutputStream));
        kryo.writeObject(output, object);
        output.close();
        return byteArrayOutputStream.toByteArray();
    }
}
