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

package org.apache.flink.datastream.impl.utils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.typeinfo.TypeDescriptors.TypeDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.types.Value;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class encapsulates the common logic for all type of streams. It can be used to handle things
 * like extract type information, create a new transformation and so on for AbstractDataStream.
 */
public final class TypeDescriptorUtils {

    public static <T> TypeInformation<?> buildTypeInfo(TypeDescriptor<T> typeDescriptor) {
        checkNotNull(typeDescriptor);

        if (typeDescriptor instanceof TypeDescriptors.ObjectArrayTypeDescriptor) {
            TypeDescriptors.ObjectArrayTypeDescriptor<T, ?> objectArrayTypeDescriptor =
                    (TypeDescriptors.ObjectArrayTypeDescriptor<T, ?>) typeDescriptor;
            return ObjectArrayTypeInfo.getInfoFor(
                    objectArrayTypeDescriptor.getTypeClass(),
                    buildTypeInfo(objectArrayTypeDescriptor.getComponentInfo()));
        } else if (typeDescriptor instanceof TypeDescriptors.BasicTypeDescriptor) {
            return BasicTypeInfo.getInfoFor(typeDescriptor.getTypeClass());
        } else if (typeDescriptor instanceof TypeDescriptors.BasicArrayTypeDescriptor) {
            return BasicArrayTypeInfo.getInfoFor(typeDescriptor.getTypeClass());
        } else if (typeDescriptor instanceof TypeDescriptors.ListTypeDescriptor) {
            return new ListTypeInfo<>(typeDescriptor.getTypeClass());
        } else if (typeDescriptor instanceof TypeDescriptors.ValueTypeDescriptor) {
            Class<T> clazz = typeDescriptor.getTypeClass();
            if (clazz != null && !Value.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("The provided class does not extend Value.");
            }
            Class<? extends Value> valueClass = (Class<? extends Value>) clazz;
            return new ValueTypeInfo<>(valueClass);
        } else if (typeDescriptor instanceof TypeDescriptors.SqlTimeTypeDescriptor) {
            return SqlTimeTypeInfo.getInfoFor(typeDescriptor.getTypeClass());
        } else if (typeDescriptor instanceof TypeDescriptors.PrimitiveArrayTypeDescriptor) {
            return PrimitiveArrayTypeInfo.getInfoFor(typeDescriptor.getTypeClass());
        } else if (typeDescriptor instanceof TypeDescriptors.PojoTypeDescriptor) {
            TypeDescriptors.PojoTypeDescriptor<T> pojoTypeDescriptor =
                    (TypeDescriptors.PojoTypeDescriptor<T>) typeDescriptor;
            Map<String, TypeDescriptor<?>> fieldsWithDescriptors = pojoTypeDescriptor.getFields();
            if (fieldsWithDescriptors == null) {
                return Types.POJO(pojoTypeDescriptor.getTypeClass());
            } else {
                Map<String, TypeInformation<?>> fieldsWithTypeInfos = new HashMap<>();
                for (Map.Entry<String, TypeDescriptor<?>> fieldWithDescriptor :
                        fieldsWithDescriptors.entrySet()) {
                    fieldsWithTypeInfos.put(
                            fieldWithDescriptor.getKey(),
                            buildTypeInfo(fieldWithDescriptor.getValue()));
                }
                return Types.POJO(pojoTypeDescriptor.getTypeClass(), fieldsWithTypeInfos);
            }
        } else {
            return TypeInformation.of(typeDescriptor.getTypeClass());
        }
    }
}
