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

package org.apache.flink.datastream.api.function;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.state.StateDeclaration;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/** Base class for all user defined process functions. */
@Experimental
public interface OneInputUsedStates extends Serializable {

    /**
     * Explicitly declares states upfront.Each specific state must be declared in this method before
     * it can be used.
     *
     * @return all declared states used by this process function.
     */
    default Set<StateDeclaration> usesStates() {
        return Collections.emptySet();
    }
}
