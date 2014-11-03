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
package com.mylife.hbase.mapper.serialization.kryo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.iq80.snappy.SnappyInputStream;
import org.iq80.snappy.SnappyOutputStream;
import org.joda.time.DateTime;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.mylife.hbase.mapper.serialization.HBaseObjectSerializer;

import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.BitSetSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.RegexSerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UUIDSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;

public class KryoSerializer implements HBaseObjectSerializer {

    private KryoSerializer() {
        // don't allow others to create new instances
    }

    public static KryoSerializer newInstance() {
        return new KryoSerializer();
    }

    @Override
    public byte[] serialize(Object object) throws IOException {
        if (object == null) {
            return null;
        }
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final Output output = new Output(new SnappyOutputStream(byteArrayOutputStream));
        getKryo().writeObject(output, object);
        output.close();

        return byteArrayOutputStream.toByteArray();

    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(byte[] byteArray, Field field) throws IOException {
        if (byteArray == null || field == null) {
            return null;
        }

        return (T) getKryo().readObject(new Input(new SnappyInputStream(new ByteArrayInputStream(byteArray))),
                conreteTypeFrom(field.getType()));
    }

    @SuppressWarnings("unchecked")
    private <T> Class<T> conreteTypeFrom(Class<T> type) {
        if (type == List.class) {
            return (Class<T>) ArrayList.class;
        }
        if (type == Map.class) {
            return (Class<T>) HashMap.class;
        }
        if (type == Collection.class) {
            return (Class<T>) ArrayList.class;
        }
        if (type == Set.class) {
            return (Class<T>) HashSet.class;
        }
        if (type == Queue.class) {
            return (Class<T>) LinkedList.class;
        }
        return type;
    }

    private Kryo getKryo() {
        final Kryo kryo = new Kryo();
        kryo.setDefaultSerializer(CompatibleFieldSerializer.class);

        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
        kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
        kryo.register(InvocationHandler.class, new JdkProxySerializer());
        kryo.register(UUID.class, new UUIDSerializer());
        kryo.register(Pattern.class, new RegexSerializer());
        kryo.register(BitSet.class, new BitSetSerializer());

        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        SynchronizedCollectionsSerializer.registerSerializers(kryo);

        // custom serializers for non-jdk libs

        // joda datetime
        kryo.register(DateTime.class, new JodaDateTimeSerializer());
        return kryo;
    }
}
