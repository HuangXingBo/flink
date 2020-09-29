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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.stream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * The Stream Arrow Python {@link AggregateFunction} Operator for ROWS clause proc-time bounded
 * OVER window.
 */
@Internal
public class StreamArrowPythonProcTimeRowsBoundedOverWindowAggregateFunctionOperator<K>
	extends AbstractStreamArrowPythonRowsBoundedOverWindowAggregateFunctionOperator<K> {

	private static final long serialVersionUID = 1L;

	private transient long currentTime;

	public StreamArrowPythonProcTimeRowsBoundedOverWindowAggregateFunctionOperator(
		Configuration config,
		long minRetentionTime,
		long maxRetentionTime,
		PythonFunctionInfo[] pandasAggFunctions,
		RowType inputType,
		RowType outputType,
		int inputTimeFieldIndex,
		long lowerBoundary,
		int[] groupingSet,
		int[] udafInputOffsets) {
		super(config, minRetentionTime, maxRetentionTime, pandasAggFunctions,
			inputType, outputType, inputTimeFieldIndex, lowerBoundary, groupingSet, udafInputOffsets);
	}

	@Override
	public void bufferInput(RowData input) throws Exception {
		currentTime = timerService.currentProcessingTime();
		// register state-cleanup timer
		registerProcessingCleanupTimer(currentTime);

		// buffer the event incoming event

		// add current element to the window list of elements with corresponding timestamp
		List<RowData> rowList = inputState.get(currentTime);
		// null value means that this is the first event received for this timestamp
		if (rowList == null) {
			rowList = new ArrayList<>();
		}
		rowList.add(input);
		inputState.put(currentTime, rowList);
	}

	@Override
	public void processElementInternal(RowData value) throws Exception {
		forwardedInputQueue.add(value);
		// gets all window data from state for the calculation
		List<RowData> inputs = inputState.get(currentTime);
		Iterable<Long> keyIter = inputState.keys();
		for (Long dataTs : keyIter) {
			insertToSortedList(dataTs);
		}
		int index = sortedTimestamps.indexOf(currentTime);
		triggerWindowProcess(inputs, inputs.size() - 1, index);
		sortedTimestamps.clear();
	}
}
