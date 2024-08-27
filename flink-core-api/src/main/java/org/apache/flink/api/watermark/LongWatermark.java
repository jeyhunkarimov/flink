/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.watermark;

import org.apache.flink.annotation.Experimental;

import java.util.Objects;

/**
 * The {@link LongWatermark} represents a watermark with a long value and an associated identifier.
 *
 * @see GeneralizedWatermark
 */
@Experimental
public class LongWatermark implements GeneralizedWatermark {
    private static final long serialVersionUID = 1L;
    private final long value;
    private final String identifier;

    /**
     * Constructs a new {@code LongWatermark} with the specified long value and identifier.
     *
     * @param value the long value of the watermark
     * @param identifier the unique identifier associated with this watermark
     */
    public LongWatermark(long value, String identifier) {
        this.value = value;
        this.identifier = identifier;
    }

    /**
     * Returns the long value of this watermark.
     *
     * @return the long value
     */
    public long getValue() {
        return value;
    }

    /**
     * Determines whether this watermark is equal to another object.
     *
     * @param o the object to compare for equality
     * @return {@code true} if the specified object is equal to this watermark; {@code false}
     *     otherwise
     */
    @Override
    public boolean equals(Object o) {
        return this == o
                || o != null
                        && o.getClass() == LongWatermark.class
                        && ((LongWatermark) o).value == this.value
                        && Objects.equals(((LongWatermark) o).identifier, this.identifier);
    }

    /**
     * Returns the hash code value for this watermark.
     *
     * @return the hash code value
     */
    @Override
    public int hashCode() {
        return Objects.hash(value, identifier);
    }

    /**
     * Returns a string representation of this watermark.
     *
     * @return a string representation of the watermark, including its value and identifier
     */
    @Override
    public String toString() {
        return "LongWatermark @ " + value + " @ " + identifier;
    }

    /**
     * Returns the identifier associated with this watermark.
     *
     * @return the identifier
     */
    @Override
    public String getIdentifier() {
        return identifier;
    }
}
