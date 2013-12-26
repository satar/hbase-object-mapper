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

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import javax.mail.internet.ContentType;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.powermock.reflect.Whitebox;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ReflectionUtils;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.mylife.hbase.mapper.annotation.HBaseField;
import com.mylife.hbase.mapper.annotation.HBaseMapField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;
import com.mylife.hbase.mapper.util.TypeHandler;

/**
 * Main HBaseMapper Class
 * 
 * 
 * @author Mike E
 */

public class HBaseEntityMapper {

    private static final transient Logger LOG = Logger.getLogger(HBaseEntityMapper.class);

    private final HTablePool hTablePool;

    @SuppressWarnings("rawtypes")
    private final ImmutableMap<Class, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod;

    @SuppressWarnings("rawtypes")
    private final ImmutableMap<Class, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod;

    @SuppressWarnings("rawtypes")
    private final ImmutableMap<Class, ? extends AccessibleObject> annotatedClassToAnnotatedHBaseRowKey;

    private final ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(
            false);

    @SuppressWarnings("unchecked")
    public HBaseEntityMapper(HTablePool hTablePool, String... basePackages) {

        this.hTablePool = hTablePool;

        @SuppressWarnings("rawtypes")
        final Builder<Class, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodBuilder = ImmutableMap
                .builder();

        @SuppressWarnings("rawtypes")
        final Builder<Class, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodBuilder = ImmutableMap
                .builder();

        @SuppressWarnings("rawtypes")
        final Builder<Class, AccessibleObject> annotatedClassToAnnotatedHBaseRowKeyBuilder = ImmutableMap.builder();

        scanner.addIncludeFilter(new AnnotationTypeFilter(HBasePersistance.class));

        for (final String basePackage : basePackages) {
            // for each package get the classes with the HBasePersistance
            // annotation
            for (final BeanDefinition beanDefinition : scanner.findCandidateComponents(basePackage)) {
                @SuppressWarnings("rawtypes")
                final Class annotatedClass;
                try {
                    annotatedClass = Class.forName(beanDefinition.getBeanClassName());
                } catch (ClassNotFoundException e) {
                    // should never happen
                    LOG.error("ClassNotFoundException while loading class for HBase entity mapper", e);
                    throw new RuntimeException(e);
                }

                //TODO add check that tablename and defaultcolumnfamily are not empty in @HBasePersistance!
                
                // figure out which method or field to use as the HBaseRowKey
                final Set<Field> hBaseRowKeyFields = Whitebox.getFieldsAnnotatedWith(
                        Whitebox.newInstance(annotatedClass), HBaseRowKey.class);
                if (hBaseRowKeyFields.size() > 1) {
                    LOG.error("@HBaseRowKey can only be used once per class. Ignoring: " + annotatedClass);
                    continue;
                }
                final Collection<Method> hBaseRowKeyMethods = Collections2.filter(
                        Arrays.asList(ReflectionUtils.getAllDeclaredMethods(annotatedClass)), new Predicate<Method>() {

                            @Override
                            public boolean apply(final Method method) {
                                return method.getAnnotation(HBaseRowKey.class) != null;
                            }
                        });
                if (hBaseRowKeyFields.size() + hBaseRowKeyMethods.size() > 1) {
                    LOG.error("@HBaseRowKey can only be used once per class. Ignoring: " + annotatedClass);
                    continue;
                }

                if (hBaseRowKeyFields.size() + hBaseRowKeyMethods.size() == 0) {
                    LOG.error("@HBaseRowKey is required. Ignoring: " + annotatedClass);
                    continue;
                }

                if (hBaseRowKeyFields.isEmpty()) {
                    final Method hBaseRowKeyMethod = hBaseRowKeyMethods.iterator().next();
                    if (hBaseRowKeyMethod.getParameterTypes().length > 0) {
                        LOG.error("@HBaseRowKey can only be used on a no arguemnt method. Ignoring: " + annotatedClass);
                        continue;
                    }
                    annotatedClassToAnnotatedHBaseRowKeyBuilder.put(annotatedClass, hBaseRowKeyMethod);
                } else {
                    annotatedClassToAnnotatedHBaseRowKeyBuilder
                            .put(annotatedClass, hBaseRowKeyFields.iterator().next());
                }

                // for each class get the fields and the corresponding getter
                annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodBuilder.put(
                        annotatedClass,
                        fieldsToGetterMap(
                                annotatedClass,
                                ImmutableSet.copyOf(Whitebox.getFieldsAnnotatedWith(
                                        Whitebox.newInstance(annotatedClass), HBaseField.class))));

                final Set<Field> hBaseMapFields = Whitebox.getFieldsAnnotatedWith(Whitebox.newInstance(annotatedClass),
                        HBaseMapField.class);

                if (hBaseMapFields.size() > 1) {
                    LOG.error("@HBaseMapField can only be used on one field per class. Ignoring: " + annotatedClass);
                    continue;
                }
                final Iterator<Field> hBaseMapFieldsIterator = hBaseMapFields.iterator();
                if (!hBaseMapFieldsIterator.hasNext()) {
                    continue;
                }
                final Field field = hBaseMapFieldsIterator.next();
                final Type[] types = ((ParameterizedType) field.getGenericType()).getActualTypeArguments();

                if (!Map.class.equals(field.getType()) || !String.class.equals((Class<?>) types[0])
                        || !String.class.equals((Class<?>) types[1])) {
                    LOG.error("@HBaseMapField can only be used on fields assignable from java.util.Map<String, String>. Ignoring: "
                            + annotatedClass);
                    continue;
                }
                annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodBuilder.put(annotatedClass,
                        fieldsToGetterMap(annotatedClass, ImmutableSet.copyOf(hBaseMapFields)));
            }
        }

        this.annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod = annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodBuilder
                .build();

        this.annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod = annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodBuilder
                .build();

        this.annotatedClassToAnnotatedHBaseRowKey = annotatedClassToAnnotatedHBaseRowKeyBuilder.build();
    }

    public void save(final Object hbasePersistableObject) throws Exception {
        final HTableInterface hTable = hTablePool.getTable(hbasePersistableObject.getClass()
                .getAnnotation(HBasePersistance.class).tableName());
        try {
            hTable.put(putsFrom(hbasePersistableObject));
        } finally {
            hTable.close();
        }
    }

    public <T extends Object> T objectFrom(final Result result, final Class<T> hBasePersistanceClass) {
        if (result.isEmpty()) {
            return null;
        }
        final T type = Whitebox.newInstance(hBasePersistanceClass);
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilyResultMap = result.getNoVersionMap();
        for (final Field field : annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod.get(
                hBasePersistanceClass).keySet()) {
            ReflectionUtils.makeAccessible(field);
            ReflectionUtils.setField(
                    field,
                    type,
                    TypeHandler.getTypedValue(
                            field.getType(),
                            columnFamilyResultMap.get(columnFamilyNameFromHBaseFieldAnnotatedField(field)).remove(
                                    Bytes.toBytes(field.getName()))));
        }
        if (annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod.get(hBasePersistanceClass) != null) {
            final Field field = annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod
                    .get(hBasePersistanceClass).keySet().iterator().next();
            final Map<String, String> map = new TreeMap<String, String>();
            for (final Entry<byte[], byte[]> entry : columnFamilyResultMap.get(
                    columnFamilyNameFromHBaseMapFieldAnnotatedField(field)).entrySet()) {
                map.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
            ReflectionUtils.makeAccessible(field);
            ReflectionUtils.setField(field, type, map);
        }
        return type;

    }

    private ImmutableMap<Field, Method> fieldsToGetterMap(final Class<?> annotatedClass,
            final ImmutableSet<Field> annotatedFields) {
        final ImmutableMap.Builder<Field, Method> mappings = new ImmutableMap.Builder<Field, Method>();
        final BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(annotatedClass);
        } catch (IntrospectionException e) {
            // should never happen
            LOG.error(e);
            throw new RuntimeException(e);
        }

        final ArrayList<PropertyDescriptor> propertyDescriptors = Lists.newArrayList(beanInfo.getPropertyDescriptors());

        for (final Field field : annotatedFields) {
            for (int i = 0; i < propertyDescriptors.size(); i++) {
                if (field.getName().equals(propertyDescriptors.get(i).getName())) {
                    mappings.put(field, propertyDescriptors.get(i).getReadMethod());
                    propertyDescriptors.remove(i);
                    i--;
                }
            }
        }

        return mappings.build();
    }

    @SuppressWarnings("unchecked")
    private ImmutableList<Put> putsFrom(final Object hbasePersistableObject) throws Exception {
        final com.google.common.collect.ImmutableList.Builder<Put> puts = ImmutableList.builder();
        final byte[] rowKey = getRowKeyFrom(hbasePersistableObject);
        if (annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod.get(hbasePersistableObject.getClass()) != null) {
            for (final Field field : annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod.get(
                    hbasePersistableObject.getClass()).keySet()) {

                puts.add(buildPut(
                        columnFamilyNameFromHBaseFieldAnnotatedField(field),
                        rowKey,
                        field.getName(),
                        fieldValue(field, hbasePersistableObject,
                                annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod)));
            }
        }
        if (annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod
                .get(hbasePersistableObject.getClass()) != null) {
            for (final Field field : annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod.get(
                    hbasePersistableObject.getClass()).keySet()) {
                puts.addAll(Iterables.transform(
                        ((Map<String, Object>) fieldValue(field, hbasePersistableObject,
                                annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod)).entrySet(),
                        new Function<Map.Entry<String, Object>, Put>() {

                            @Override
                            public Put apply(Entry<String, Object> input) {
                                return buildPut(columnFamilyNameFromHBaseMapFieldAnnotatedField(field), rowKey, input.getKey(), input.getValue());
                            }
                        }));
            }
        }
        return puts.build();
    }

    private byte[] getRowKeyFrom(final Object hbasePersistableObject) throws Exception {
        AccessibleObject accessibleObject = annotatedClassToAnnotatedHBaseRowKey.get(hbasePersistableObject.getClass());
        accessibleObject.setAccessible(true);
        if (accessibleObject.getClass().isAssignableFrom(Method.class)) {

            return toBytes(((Method) accessibleObject).invoke(hbasePersistableObject, (Object[]) null));
        } else {
            return toBytes(((Field) accessibleObject).get(hbasePersistableObject));
        }
    }

    private Put buildPut(final byte[] columnFamilyName, final byte[] rowKey, final String qualifierName,
            final Object qualifierValue) {
        final Put put = new Put(rowKey);
        put.add(columnFamilyName, Bytes.toBytes(qualifierName), toBytes(qualifierValue));
        return put;
    }

    private byte[] defaultColumnFamilyNameFrom(final Class<?> hBasePersistanceClass) {
        return Bytes.toBytes(hBasePersistanceClass.getAnnotation(HBasePersistance.class).defaultColumnFamilyName());
    }

    private byte[] defaultColumnFamilyNameFrom(final Object hbasePersistableObject) {
        return defaultColumnFamilyNameFrom(hbasePersistableObject.getClass());
    }

    private byte[] columnFamilyNameFromHBaseFieldAnnotatedField(final Field hbaseFieldAnnotatedField) {
        return hbaseFieldAnnotatedField.getAnnotation(HBaseField.class).columnFamilyName().isEmpty() ? defaultColumnFamilyNameFrom(hbaseFieldAnnotatedField
                .getDeclaringClass()) : Bytes.toBytes(hbaseFieldAnnotatedField.getAnnotation(HBaseField.class)
                .columnFamilyName());
    }

    private byte[] columnFamilyNameFromHBaseMapFieldAnnotatedField(final Field hbaseMapFieldAnnotatedField) {
        return hbaseMapFieldAnnotatedField.getAnnotation(HBaseMapField.class).columnFamilyName().isEmpty() ? defaultColumnFamilyNameFrom(hbaseMapFieldAnnotatedField
                .getDeclaringClass()) : Bytes.toBytes(hbaseMapFieldAnnotatedField.getAnnotation(HBaseMapField.class)
                .columnFamilyName());
    }

    private Object fieldValue(final Field field, final Object hbasePersistableObject,
            @SuppressWarnings("rawtypes") final ImmutableMap<Class, ImmutableMap<Field, Method>> map) {
        final Method getter = map.get(hbasePersistableObject.getClass()).get(field);
        ReflectionUtils.makeAccessible(getter);
        return ReflectionUtils.invokeMethod(getter, hbasePersistableObject);
    }

    private byte[] toBytes(final Object value) {
        if (value == null) {
            return null;
        } else if (value.getClass().isAssignableFrom(Integer.class)) {
            return Bytes.toBytes((Integer) value);
        } else if (value.getClass().isAssignableFrom(Long.class)) {
            return Bytes.toBytes((Long) value);
        } else if (value.getClass().isAssignableFrom(String.class)) {
            return Bytes.toBytes((String) value);
        } else if (value.getClass().isAssignableFrom(Boolean.class)) {
            return Bytes.toBytes((Boolean) value);
        } else if (value.getClass().isAssignableFrom(byte[].class)) {
            return (byte[]) value;
        } else if (value.getClass().isEnum()) {
            return Bytes.toBytes(((Enum<?>) value).name());
        } else if (value.getClass().isAssignableFrom(ContentType.class)) {
            return Bytes.toBytes(((ContentType) value).toString());
        } else {
            throw new IllegalArgumentException("Unknow type: " + value.getClass()
                    + " please add handling for this type");
        }
    }
}