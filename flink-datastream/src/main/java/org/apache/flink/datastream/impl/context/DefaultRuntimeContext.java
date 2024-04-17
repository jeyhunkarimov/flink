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

package org.apache.flink.datastream.impl.context;

import org.apache.flink.datastream.api.context.JobInfo;
import org.apache.flink.datastream.api.context.ProcessingTimeManager;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.context.TaskInfo;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.util.function.Consumer;
import java.util.function.Supplier;

/** The default implementation of {@link RuntimeContext}. */
public class DefaultRuntimeContext implements RuntimeContext {
    private final DefaultJobInfo jobInfo;

    private final DefaultTaskInfo taskInfo;

    private final DefaultStateManager stateManager;

    private final ProcessingTimeManager processingTimeManager;

    private final MetricGroup metricGroup;

    public DefaultRuntimeContext(
            StreamingRuntimeContext operatorContext,
            int parallelism,
            int maxParallelism,
            String taskName,
            Supplier<Object> currentKeySupplier,
            Consumer<Object> currentKeySetter,
            ProcessingTimeManager processingTimeManager) {
        this.jobInfo = new DefaultJobInfo(operatorContext);
        this.taskInfo = new DefaultTaskInfo(parallelism, maxParallelism, taskName);
        this.stateManager = new DefaultStateManager(currentKeySupplier, currentKeySetter);
        this.processingTimeManager = processingTimeManager;
        this.metricGroup = operatorContext.getMetricGroup();
    }

    @Override
    public JobInfo getJobInfo() {
        return jobInfo;
    }

    @Override
    public TaskInfo getTaskInfo() {
        return taskInfo;
    }

    @Override
    public DefaultStateManager getStateManager() {
        return stateManager;
    }

    public ProcessingTimeManager getProcessingTimeManager() {
        return processingTimeManager;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return metricGroup;
    }
}
