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
import java.io.IOException;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import javax.mail.internet.ContentType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.powermock.reflect.Whitebox;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.ReflectionUtils;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.mylife.hbase.mapper.annotation.HBaseField;
import com.mylife.hbase.mapper.annotation.HBaseMapField;
import com.mylife.hbase.mapper.annotation.HBaseObjectField;
import com.mylife.hbase.mapper.annotation.HBasePersistance;
import com.mylife.hbase.mapper.annotation.HBaseRowKey;
import com.mylife.hbase.mapper.util.TypeHandler;

/**
 * Main HBaseMapper Class
 * 
 * @author Mike E
 */

public class HBaseEntityMapper {

    private static final transient Logger LOG = Logger.getLogger(HBaseEntityMapper.class);

    private final HTablePool hTablePool;

    private final ImmutableMap<Class<?>, ? extends AccessibleObject> annotatedClassToAnnotatedHBaseRowKey;

    private final ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod;

    private final ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod;

    private final ImmutableMap<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod;

    private final ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(
            false);

    /**
     * Constructor: Sets up hTablePool and scans base package paths looking for classes annotated with @HBasePersistance
     * 
     * @param hTablePool
     * @param basePackages
     */
    @SuppressWarnings("unchecked")
    public HBaseEntityMapper(HTablePool hTablePool, String... basePackages) {

        final HBaseAdmin hBaseAdmin;
        try {
            hBaseAdmin = new HBaseAdmin((Configuration) Whitebox.getInternalState(hTablePool, "config"));
        } catch (IOException e) {
            LOG.fatal("Could not connect to HBase failing HBase object mapping!", e);
            this.hTablePool = null;
            this.annotatedClassToAnnotatedHBaseRowKey = null;
            this.annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod = null;
            this.annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod = null;
            this.annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod = null;
            return;
        }

        this.hTablePool = hTablePool;

        final Map<Class<?>, AccessibleObject> annotatedClassToAnnotatedHBaseRowKeyMap = new HashMap<Class<?>, AccessibleObject>();

        final Map<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodMap = new HashMap<Class<?>, ImmutableMap<Field, Method>>();

        final Map<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethodMap = new HashMap<Class<?>, ImmutableMap<Field, Method>>();

        final Map<Class<?>, ImmutableMap<Field, Method>> annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodMap = new HashMap<Class<?>, ImmutableMap<Field, Method>>();

        /*
         * Only include classes annotated with @HBasePersistance
         */
        scanner.addIncludeFilter(new AnnotationTypeFilter(HBasePersistance.class));

        for (final String basePackage : basePackages) {

            for (final BeanDefinition beanDefinition : scanner.findCandidateComponents(basePackage)) {
                final Class<?> annotatedClass;
                try {
                    annotatedClass = Class.forName(beanDefinition.getBeanClassName());
                } catch (ClassNotFoundException e) {
                    // should never happen
                    LOG.error("ClassNotFoundException while loading class for HBase entity mapper", e);
                    throw new RuntimeException(e);
                }
                /*
                 * Get the hbase table name - required!
                 */
                final String tableName = annotatedClass.getAnnotation(HBasePersistance.class).tableName();
                if (StringUtils.isEmpty(tableName)) {
                    LOG.error("@HBasePersistance must have a non-empty tableName. Ignoring: " + annotatedClass);
                    continue;
                }

                try {
                    if (!hBaseAdmin.tableExists(tableName)) {
                        LOG.error("table " + tableName + " in @HBasePersistance.tableName() does not exist. Ignoring: "
                                + annotatedClass);
                        continue;
                    }
                } catch (IOException e) {
                    LOG.error("Could not verify table " + tableName
                            + "in @HBasePersistance.tableName() exists. Ignoring: " + annotatedClass, e);
                    continue;
                }

                /*
                 * Get the default hbase column family name. This will be used as default column family where fields are
                 * mapped to but not required if all annotated fields specifically mention the column family they belong
                 * to.
                 * 
                 * TODO: Current implementation makes this field required but it really isn't based on my
                 * comment made previously. That is, if all annotated fields specifically mention the column family
                 * where they will be found then a default column family name is unneeded.
                 */
                final String defaultColumnFamilyName = annotatedClass.getAnnotation(HBasePersistance.class)
                        .defaultColumnFamilyName();
                if (StringUtils.isEmpty(defaultColumnFamilyName)) {
                    LOG.error("@HBasePersistance must have a non-empty defaultColumnFamilyName. Ignoring: "
                            + annotatedClass);
                    continue;
                }

                try {
                    /*
                     * Confirm the hbase table and column family exists
                     */
                    if (!hBaseAdmin.getTableDescriptor(Bytes.toBytes(tableName)).hasFamily(
                            Bytes.toBytes(defaultColumnFamilyName))) {
                        LOG.error("defaultColumnFamilyName (" + defaultColumnFamilyName + ") in " + tableName
                                + "in @HBasePersistance.defaultColumnFamilyName() does not exist. Ignoring: "
                                + annotatedClass);
                        continue;
                    }
                } catch (IOException e) {
                    LOG.error("Could not verify defaultColumnFamilyName (" + defaultColumnFamilyName + ") in "
                            + tableName + "in @HBasePersistance.defaultColumnFamilyName() exists. Ignoring: "
                            + annotatedClass, e);
                    continue;
                }

                /*
                 * @HBaseField : Find fields with this annotation and their corresponding getter method
                 */
                Set<Field> hBaseFieldAnnotatedFieldsSet = Whitebox.getFieldsAnnotatedWith(
                        Whitebox.newInstance(annotatedClass), HBaseField.class);

                /*
                 * Use a Predicate to look through all @HBaseField annotated fields and skip whole class if even one
                 * field is not supported
                 */
                if (CollectionUtils.exists(hBaseFieldAnnotatedFieldsSet,
                        new org.apache.commons.collections.Predicate() {

                            @Override
                            public boolean evaluate(Object object) {

                                return !TypeHandler.supports(((Field) object).getType());
                            }
                        })) {
                    LOG.error("Unsupported type annotated with @HBaseField. Ignoring: " + annotatedClass);
                    continue;
                }

                annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodMap.put(annotatedClass,
                        fieldsToGetterMap(annotatedClass, ImmutableSet.copyOf(hBaseFieldAnnotatedFieldsSet)));

                /*
                 * @HBaseObjectField : Find all fields with this annotation and their corresponding getter method
                 */
                annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethodMap.put(
                        annotatedClass,
                        fieldsToGetterMap(
                                annotatedClass,
                                ImmutableSet.copyOf(Whitebox.getFieldsAnnotatedWith(
                                        Whitebox.newInstance(annotatedClass), HBaseObjectField.class))));

                /*
                 * @HBaseMapField : See if this annotation exists and if so, make sure there is only one
                 */
                final Set<Field> hBaseMapFieldAnnotatedFieldsSet = Whitebox.getFieldsAnnotatedWith(
                        Whitebox.newInstance(annotatedClass), HBaseMapField.class);

                if (hBaseMapFieldAnnotatedFieldsSet.size() > 1) {
                    LOG.error("@HBaseMapField can only be used on one field per class. Ignoring: " + annotatedClass);
                    continue;
                }
                final Iterator<Field> hBaseMapFieldsIterator = hBaseMapFieldAnnotatedFieldsSet.iterator();
                if (hBaseMapFieldsIterator.hasNext()) {
                    final Field field = hBaseMapFieldsIterator.next();
                    final Type[] types = TypeHandler.getGenericTypesFromField(field);

                    if ((Modifier.isAbstract(field.getType().getModifiers()) && !Map.class.equals(field.getType()))
                            || !Map.class.isAssignableFrom(field.getType())
                            || !String.class.equals((Class<?>) types[0]) || !String.class.equals((Class<?>) types[1])) {
                        LOG.error("@HBaseMapField must be used on a field of type java.util.Map<String, String> OR a concrete fields assignable from java.util.Map<String, String> . Ignoring: "
                                + annotatedClass);
                        continue;
                    }
                    annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodMap.put(annotatedClass,
                            fieldsToGetterMap(annotatedClass, ImmutableSet.copyOf(hBaseMapFieldAnnotatedFieldsSet)));

                }

                /*
                 * Figure out which method or field to use as the HBaseRowKey this has to be at the end since
                 * 
                 * @HBaseRowKey is required in the class we can use this to key the other maps we are collecting
                 */
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
                    annotatedClassToAnnotatedHBaseRowKeyMap.put(annotatedClass, hBaseRowKeyMethod);
                } else {
                    annotatedClassToAnnotatedHBaseRowKeyMap.put(annotatedClass, hBaseRowKeyFields.iterator().next());
                }
            }
        }

        /*
         * keep only the valid classed in our maps
         */
        annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodMap.keySet().retainAll(
                annotatedClassToAnnotatedHBaseRowKeyMap.keySet());
        annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethodMap.keySet().retainAll(
                annotatedClassToAnnotatedHBaseRowKeyMap.keySet());
        annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodMap.keySet().retainAll(
                annotatedClassToAnnotatedHBaseRowKeyMap.keySet());

        // set our state
        this.annotatedClassToAnnotatedHBaseRowKey = ImmutableMap.copyOf(annotatedClassToAnnotatedHBaseRowKeyMap);
        this.annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod = ImmutableMap
                .copyOf(annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethodMap);
        this.annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod = ImmutableMap
                .copyOf(annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethodMap);
        this.annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod = ImmutableMap
                .copyOf(annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethodMap);
    }

    public void save(final Object hbasePersistableObject) throws IllegalArgumentException, Exception {
        if (!annotatedClassToAnnotatedHBaseRowKey.containsKey(hbasePersistableObject.getClass())) {
            throw new IllegalArgumentException(
                    "Object passed to save(final Object hbasePersistableObject) must be of a correct HBase persistable class! If this class is annotated with @HBasePersistance please see start-up errors for why it might have been excluded. ");
        }
        final HTableInterface hTable = hTablePool.getTable(hbasePersistableObject.getClass()
                .getAnnotation(HBasePersistance.class).tableName());
        try {
            hTable.put(putsFrom(hbasePersistableObject));
        } finally {
            hTable.close();
        }
    }

    public void deleteByRow(final Object hbasePersistableObject) throws Exception {
        final HTableInterface hTable = hTablePool.getTable(hbasePersistableObject.getClass()
                .getAnnotation(HBasePersistance.class).tableName());
        final byte[] rowKey = getRowKeyFrom(hbasePersistableObject);
        try {
            hTable.delete(new Delete(rowKey));
        } finally {
            hTable.close();
        }
    }

    @SuppressWarnings("unchecked")
    public void deleteByColumn(final Object hbasePersistableObject) throws Exception {
        final HTableInterface hTable = hTablePool.getTable(hbasePersistableObject.getClass()
                .getAnnotation(HBasePersistance.class).tableName());
        final byte[] rowKey = getRowKeyFrom(hbasePersistableObject);
        final Delete delete = new Delete(rowKey);
        try {
            /*
             * Add column deletes for @HBaseField(s)
             */
            if (annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod.get(hbasePersistableObject
                    .getClass()) != null) {
                for (final Field field : annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod.get(
                        hbasePersistableObject.getClass()).keySet()) {
                    addToDeleteColumn(delete, columnFamilyNameFromHBaseFieldAnnotatedField(field), field.getName());
                }
            }
            /*
             * Add column deletes for @HBaseObject(s)
             */
            if (annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod.get(hbasePersistableObject
                    .getClass()) != null) {
                for (final Field field : annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod.get(
                        hbasePersistableObject.getClass()).keySet()) {
                    addToDeleteColumn(delete, columnFamilyNameFromHBaseObjectFieldAnnotatedField(field),
                            field.getName());
                }
            }
            /*
             * Add column deletes for HBaseMapField -- we make sure there is at max one of these earlier in the logic
             * flow
             */
            if (annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod.get(hbasePersistableObject
                    .getClass()) != null) {
                for (final Field field : annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod.get(
                        hbasePersistableObject.getClass()).keySet()) {
                    for (final Map.Entry<String, Object> entry : ((Map<String, Object>) fieldValue(field,
                            hbasePersistableObject,
                            annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod)).entrySet()) {
                        addToDeleteColumn(delete, columnFamilyNameFromHBaseMapFieldAnnotatedField(field),
                                entry.getKey());
                    }
                }
            }
            hTable.delete(delete);
        } finally {
            hTable.close();
        }
    }

    public <T> void delete(final Set<T> hbasePersistableObjects, final Class<T> hBasePersistanceClass) throws Exception {
        if (hbasePersistableObjects == null || hbasePersistableObjects.isEmpty()) {
            return;
        }
        final HTableInterface hTable = hTablePool.getTable(hBasePersistanceClass.getAnnotation(HBasePersistance.class)
                .tableName());
        List<Delete> list = new ArrayList<Delete>();
        for (T hbaseObject : hbasePersistableObjects) {
            final byte[] rowKey = getRowKeyFrom(hbaseObject);
            list.add(new Delete(rowKey));
        }
        try {
            hTable.delete(list);
        } finally {
            hTable.close();
        }
    }

    public <T> ImmutableList<T> objectListFrom(final Result[] results, final Class<T> hBasePersistanceClass) {
        return objectListFromIterable(Arrays.asList(results), hBasePersistanceClass);
    }

    public <T> ImmutableList<T> objectListFrom(final ResultScanner results, final Class<T> hBasePersistanceClass) {
        return objectListFromIterable(results, hBasePersistanceClass);
    }

    private <T> ImmutableList<T> objectListFromIterable(final Iterable<Result> results,
            final Class<T> hBasePersistanceClass) {
        Builder<T> builder = ImmutableList.builder();
        for (final Result result : results) {
            builder.add(objectFrom(result, hBasePersistanceClass));
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    public <T> T objectFrom(final Result result, final Class<T> hBasePersistanceClass) {
        if (!annotatedClassToAnnotatedHBaseRowKey.containsKey(hBasePersistanceClass)) {
            throw new IllegalArgumentException(
                    "Class passed to objectFrom(final Result result, final Class<T> hBasePersistanceClass) must be a correct HBase persistable class! If this class is annotaed with @HBasePersistance please see startup errors for why it might have been excluded. ");
        }
        if (result.isEmpty()) {
            return null;
        }
        /*
         * Create new instance of our target class
         */
        final T type = Whitebox.newInstance(hBasePersistanceClass);
        final NavigableMap<byte[], NavigableMap<byte[], byte[]>> columnFamilyResultMap = result.getNoVersionMap();
        /*
         * Map results to fields annotated with @HBaseField
         */
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
        /*
         * Map results to fields annotated with @HBaseObjectField
         */
        for (final Field field : annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod.get(
                hBasePersistanceClass).keySet()) {
            ReflectionUtils.makeAccessible(field);
            try {
                /*
                 * We may purposely not want to populate certain fields by not including the column family
                 */
                final byte[] columnFamily = columnFamilyNameFromHBaseObjectFieldAnnotatedField(field);
                if (columnFamilyResultMap.containsKey(columnFamily)) {
                    ReflectionUtils.setField(
                            field,
                            type,
                            field.getAnnotation(HBaseObjectField.class)
                                    .serializationStategy()
                                    .deserialize(
                                            columnFamilyResultMap.get(columnFamily).remove(
                                                    Bytes.toBytes(field.getName())), field));
                }
            } catch (Exception e) {
                /*
                 *  We serialized this we should be able to de-serialize it. Did the serialization change?
                 *  
                 *  TODO: store serialization type so we can better guarantee de-serialization
                 */
                LOG.error(
                        "Could not deserialize "
                                + field.getName()
                                + " for family map name: "
                                + columnFamilyNameFromHBaseObjectFieldAnnotatedField(field)
                                + " Did the serialization (KRYO/JSON) OR field type change from when you serialized the object?",
                        e);
            }
        }
        /*
         * Map results to fields annotated with @HBaseMapField
         */
        mapFieldBlock: {
            if (annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod.get(hBasePersistanceClass) != null) {
                final Field field = annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod
                        .get(hBasePersistanceClass).keySet().iterator().next();
                Map<String, String> map;
                if (field.getType().equals(Map.class)) {
                    /*
                     *  If the object just calls for a Map give them a TreeMap();
                     */
                    map = new TreeMap<String, String>();
                } else {
                    /*
                     *  else try to create an instance of the Map class they are using
                     */
                    try {
                        map = (Map<String, String>) Whitebox.getConstructor(field.getType()).newInstance(
                                (Object[]) null);
                    } catch (Exception e) {
                        /*
                         *  Done our best to guard against this but still possible
                         */
                        LOG.error("Could not create new instance of map.", e);
                        break mapFieldBlock;
                    }
                }
                for (final Entry<byte[], byte[]> entry : columnFamilyResultMap.get(
                        columnFamilyNameFromHBaseMapFieldAnnotatedField(field)).entrySet()) {
                    map.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
                }
                ReflectionUtils.makeAccessible(field);
                ReflectionUtils.setField(field, type, map);
            }
        }

        return type;

    }

    /**
     * Helper method to lookup getter methods for each annotated Field
     * 
     * Note: This requires a proper bean pattern getter method in the annotatedClass for the annotatedField.
     * 
     * @param annotatedClass
     * @param annotatedFields
     * @return
     */
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
        final Put put = new Put(getRowKeyFrom(hbasePersistableObject));
        /*
         * Add puts for @HBaseField(s)
         */
        if (annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod.get(hbasePersistableObject.getClass()) != null) {
            for (final Field field : annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod.get(
                    hbasePersistableObject.getClass()).keySet()) {
                addToPut(
                        put,
                        columnFamilyNameFromHBaseFieldAnnotatedField(field),

                        field.getName(),
                        fieldValue(field, hbasePersistableObject,
                                annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod));
            }
        }
        /*
         * Add puts for @HBaseObject(s)
         */
        if (annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod.get(hbasePersistableObject
                .getClass()) != null) {
            for (final Field field : annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod.get(
                    hbasePersistableObject.getClass()).keySet()) {
                addToPut(
                        put,
                        columnFamilyNameFromHBaseObjectFieldAnnotatedField(field),
                        field.getName(),
                        field.getAnnotation(HBaseObjectField.class)
                                .serializationStategy()
                                .serialize(
                                        fieldValue(field, hbasePersistableObject,
                                                annotatedClassToAnnotatedObjectFieldMappingWithCorrespondingGetterMethod)));

            }
        }
        /*
         * Add puts for HBaseMapField -- we make sure there is at max one of these earlier in the logic flow
         */
        if (annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod
                .get(hbasePersistableObject.getClass()) != null) {
            for (final Field field : annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod.get(
                    hbasePersistableObject.getClass()).keySet()) {
                for (final Map.Entry<String, Object> entry : ((Map<String, Object>) fieldValue(field,
                        hbasePersistableObject, annotatedClassToAnnotatedMapFieldMappingWithCorrespondingGetterMethod))
                        .entrySet()) {
                    addToPut(put, columnFamilyNameFromHBaseMapFieldAnnotatedField(field), entry.getKey(),
                            entry.getValue());
                }
            }
        }

        return ImmutableList.of(put);
    }

    /**
     * Gets indexable fields data as field-value map for simple field types. Important to note is it will only include
     * fields that have actual values. If the value of a given indexable field is null, it will not be included.
     * 
     * NOTE: This only returns simple field data from a field annotated with @HBaseField. It does not include complex
     * object fields annotated with @HBaseObjectField with could also be indexable.
     * 
     * @param hbasePersistableObject
     * @return
     * @throws Exception
     */
    public ImmutableMap<byte[], byte[]> getIndexableFields(final Object hbasePersistableObject) throws Exception {
        final ImmutableMap.Builder<byte[], byte[]> data = new ImmutableMap.Builder<byte[], byte[]>();

        /*
         * Iterate over field and extract indexable data only.
         */
        if (annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod.get(hbasePersistableObject.getClass()) != null) {
            for (final Field field : annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod.get(
                    hbasePersistableObject.getClass()).keySet()) {
                if (field.getAnnotation(HBaseField.class).indexable()) {
                    final byte[] value = toBytes(fieldValue(field, hbasePersistableObject,
                            annotatedClassToAnnotatedFieldMappingWithCorrespondingGetterMethod));
                    if (value != null) {
                        data.put(Bytes.toBytes(field.getName()), value);
                    }
                }
            }
        }

        return data.build();
    }

    private byte[] getRowKeyFrom(final Object hbasePersistableObject) throws Exception {
        /*
         * Get the field or method that has the @HBaseRowKey annotation.
         */
        AccessibleObject accessibleObject = annotatedClassToAnnotatedHBaseRowKey.get(hbasePersistableObject.getClass());
        accessibleObject.setAccessible(true);
        /*
         * Either invoke a method to get the key value from the overall hbasePersistableObject or simple field value.
         */
        if (accessibleObject.getClass().isAssignableFrom(Method.class)) {
            return toBytes(((Method) accessibleObject).invoke(hbasePersistableObject, (Object[]) null));
        } else {
            return toBytes(((Field) accessibleObject).get(hbasePersistableObject));
        }
    }

    private void addToDeleteColumn(final Delete delete, final byte[] columnFamilyName, final String qualifierName) {
        if (columnFamilyName == null || qualifierName == null) {
            /*
             * all of the arguments are required don't want to persist a null value
             */
            return;
        }
        delete.deleteColumn(columnFamilyName, Bytes.toBytes(qualifierName), HConstants.LATEST_TIMESTAMP);
        return;
    }

    private void addToPut(final Put put, final byte[] columnFamilyName, final String qualifierName,
            final Object qualifierValue) {
        if (columnFamilyName == null || qualifierName == null || qualifierValue == null) {
            /*
             * all of the arguments are required don't want to persist a null value
             */
            return;
        }

        put.add(columnFamilyName, Bytes.toBytes(qualifierName), toBytes(qualifierValue));
        return;
    }

    private byte[] defaultColumnFamilyNameFrom(final Class<?> hBasePersistanceClass) {
        return Bytes.toBytes(hBasePersistanceClass.getAnnotation(HBasePersistance.class).defaultColumnFamilyName());
    }

    /*
     *  TODO cache this up front that will (maybe?) speed things up and allow for verifying all column family names ahead of time
     */
    private byte[] columnFamilyNameFromHBaseFieldAnnotatedField(final Field hbaseFieldAnnotatedField) {
        return hbaseFieldAnnotatedField.getAnnotation(HBaseField.class).columnFamilyName().isEmpty() ? defaultColumnFamilyNameFrom(hbaseFieldAnnotatedField
                .getDeclaringClass())
                : Bytes.toBytes(hbaseFieldAnnotatedField.getAnnotation(HBaseField.class)
                        .columnFamilyName());
    }

    /*
     *  TODO cache this up front that will (maybe?) speed things up and allow for verifying all column family names ahead of time
     */
    private byte[] columnFamilyNameFromHBaseMapFieldAnnotatedField(final Field hbaseMapFieldAnnotatedField) {
        return hbaseMapFieldAnnotatedField.getAnnotation(HBaseMapField.class).columnFamilyName().isEmpty() ? defaultColumnFamilyNameFrom(hbaseMapFieldAnnotatedField
                .getDeclaringClass())
                : Bytes.toBytes(hbaseMapFieldAnnotatedField.getAnnotation(HBaseMapField.class)
                        .columnFamilyName());
    }

    /*
     *  TODO cache this up front that will (maybe?) speed things up and allow for verifying all column family names ahead of time
     */
    private byte[] columnFamilyNameFromHBaseObjectFieldAnnotatedField(final Field hbaseMapFieldAnnotatedField) {
        return hbaseMapFieldAnnotatedField.getAnnotation(HBaseObjectField.class).columnFamilyName().isEmpty() ? defaultColumnFamilyNameFrom(hbaseMapFieldAnnotatedField
                .getDeclaringClass())
                : Bytes.toBytes(hbaseMapFieldAnnotatedField.getAnnotation(HBaseObjectField.class)
                        .columnFamilyName());
    }

    private Object fieldValue(final Field field, final Object hbasePersistableObject,
            final ImmutableMap<Class<?>, ImmutableMap<Field, Method>> map) {
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
        } else if (value.getClass().isAssignableFrom(Short.class)) {
            return Bytes.toBytes((Short) value);
        } else if (value.getClass().isAssignableFrom(Float.class)) {
            return Bytes.toBytes((Float) value);
        } else if (value.getClass().isAssignableFrom(Double.class)) {
            return Bytes.toBytes((Double) value);
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