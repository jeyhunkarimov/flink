/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/** Utility class to create instances from class objects and checking failure reasons. */
@Internal
public final class InstantiationUtil extends InstantiationUtilBase {

    private static final Logger LOG = LoggerFactory.getLogger(InstantiationUtil.class);

    /**
     * A mapping between the full path of a deprecated serializer and its equivalent. These mappings
     * are hardcoded and fixed.
     *
     * <p>IMPORTANT: mappings can be removed after 1 release as there will be a "migration path". As
     * an example, a serializer is removed in 1.5-SNAPSHOT, then the mapping should be added for
     * 1.5, and it can be removed in 1.6, as the path would be Flink-{< 1.5} -> Flink-1.5 ->
     * Flink-{>= 1.6}.
     */
    private enum MigrationUtil {

        // To add a new mapping just pick a name and add an entry as the following:

        HASH_MAP_SERIALIZER(
                "org.apache.flink.runtime.state.HashMapSerializer",
                ObjectStreamClass.lookup(MapSerializer.class)); // added in 1.5

        /**
         * An internal unmodifiable map containing the mappings between deprecated and new
         * serializers.
         */
        private static final Map<String, ObjectStreamClass> EQUIVALENCE_MAP =
                Collections.unmodifiableMap(initMap());

        /** The full name of the class of the old serializer. */
        private final String oldSerializerName;

        /** The serialization descriptor of the class of the new serializer. */
        private final ObjectStreamClass newSerializerStreamClass;

        MigrationUtil(String oldSerializerName, ObjectStreamClass newSerializerStreamClass) {
            this.oldSerializerName = oldSerializerName;
            this.newSerializerStreamClass = newSerializerStreamClass;
        }

        private static Map<String, ObjectStreamClass> initMap() {
            final Map<String, ObjectStreamClass> init =
                    CollectionUtil.newHashMapWithExpectedSize(4);
            for (MigrationUtil m : MigrationUtil.values()) {
                init.put(m.oldSerializerName, m.newSerializerStreamClass);
            }
            return init;
        }

        private static ObjectStreamClass getEquivalentSerializer(String classDescriptorName) {
            return EQUIVALENCE_MAP.get(classDescriptorName);
        }
    }

    @Nullable
    public static <T> T readObjectFromConfig(Configuration config, String key, ClassLoader cl)
            throws IOException, ClassNotFoundException {
        byte[] bytes = config.getBytes(key, null);
        if (bytes == null) {
            return null;
        }

        return deserializeObject(bytes, cl);
    }

    public static void writeObjectToConfig(Object o, Configuration config, String key)
            throws IOException {
        byte[] bytes = serializeObject(o);
        config.setBytes(key, bytes);
    }

    public static <T> byte[] serializeToByteArray(TypeSerializer<T> serializer, T record)
            throws IOException {
        if (record == null) {
            throw new NullPointerException("Record to serialize to byte array must not be null.");
        }

        ByteArrayOutputStream bos = new ByteArrayOutputStream(64);
        DataOutputViewStreamWrapper outputViewWrapper = new DataOutputViewStreamWrapper(bos);
        serializer.serialize(record, outputViewWrapper);
        return bos.toByteArray();
    }

    public static <T> T deserializeFromByteArray(TypeSerializer<T> serializer, byte[] buf)
            throws IOException {
        if (buf == null) {
            throw new NullPointerException("Byte array to deserialize from must not be null.");
        }

        DataInputViewStreamWrapper inputViewWrapper =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(buf));
        return serializer.deserialize(inputViewWrapper);
    }

    public static <T> T deserializeFromByteArray(TypeSerializer<T> serializer, T reuse, byte[] buf)
            throws IOException {
        if (buf == null) {
            throw new NullPointerException("Byte array to deserialize from must not be null.");
        }

        DataInputViewStreamWrapper inputViewWrapper =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(buf));
        return serializer.deserialize(reuse, inputViewWrapper);
    }

    /**
     * Clones the given writable using the {@link IOReadableWritable serialization}.
     *
     * @param original Object to clone
     * @param <T> Type of the object to clone
     * @return Cloned object
     * @throws IOException Thrown is the serialization fails.
     */
    public static <T extends IOReadableWritable> T createCopyWritable(T original)
            throws IOException {
        if (original == null) {
            return null;
        }

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
            original.write(out);
        }

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        try (DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {

            @SuppressWarnings("unchecked")
            T copy = (T) instantiate(original.getClass());
            copy.read(in);
            return copy;
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Private constructor to prevent instantiation. */
    private InstantiationUtil() {
        throw new RuntimeException();
    }
}
