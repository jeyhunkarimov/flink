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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.Experimental;

import java.io.Serializable;
import java.util.Map;

@Experimental
public class TypeDescriptors implements Serializable {

    /**
     * Descriptor interface to create TypeInformation instances.
     *
     * @param <T> The type represented by this type descriptor.
     */
    @Experimental
    public interface TypeDescriptor<T> extends Serializable {

        /**
         * Gets the class of the type represented by this type descriptor.
         *
         * @return The class of the type represented by this type descriptor.
         */
        Class<T> getTypeClass();
    }

    /**
     * Descriptor interface to create BasicArrayTypeInfo instances.
     *
     * @param <T> The type represented by this type descriptor.
     */
    @Experimental
    public interface BasicArrayTypeDescriptor<T> extends TypeDescriptor<T> {}

    /**
     * Descriptor interface to create ListTypeInfo instances.
     *
     * @param <T> The type represented by this type descriptor.
     */
    @Experimental
    public interface ListTypeDescriptor<T> extends TypeDescriptor<T> {}

    /**
     * Descriptor interface to create ValueTypeInfo instances.
     *
     * @param <T> The type represented by this type descriptor.
     */
    @Experimental
    public interface ValueTypeDescriptor<T> extends TypeDescriptor<T> {}

    /** Descriptor interface to create SqlTimeTypeInfo instances. */
    @Experimental
    public interface SqlTimeTypeDescriptor<T> extends TypeDescriptor<T> {}

    /** Descriptor interface to create PrimitiveArrayTypeInfo instances. */
    @Experimental
    public interface PrimitiveArrayTypeDescriptor<T> extends TypeDescriptor<T> {}

    /**
     * Descriptor interface to create PojoTypeInfo instances.
     *
     * @param <T> The type represented by this type descriptor.
     */
    @Experimental
    public interface PojoTypeDescriptor<T> extends TypeDescriptor<T> {

        /**
         * Get Map of field names with their type descriptors.
         *
         * @return Map of field names with their type descriptors.
         */
        Map<String, TypeDescriptor<?>> getFields();
    }

    /**
     * Descriptor interface to create BasicTypeInfo instances.
     *
     * @param <T> The type represented by this type descriptor.
     */
    @Experimental
    public interface BasicTypeDescriptor<T> extends TypeDescriptor<T> {}

    /**
     * Descriptor interface to create ObjectArrayTypeInfo instances.
     *
     * @param <T> The type represented by this type descriptor.
     * @param <C> The type of array component.
     */
    @Experimental
    public interface ObjectArrayTypeDescriptor<T, C> extends TypeDescriptor<T> {

        /** TODO. */
        TypeDescriptor<C> getComponentInfo();
    }
}
