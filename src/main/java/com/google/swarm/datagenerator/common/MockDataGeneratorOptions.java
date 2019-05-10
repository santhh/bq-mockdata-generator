/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.swarm.datagenerator.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface MockDataGeneratorOptions extends PipelineOptions {
	@Description("Windowed interval")
	@Default.Integer(1)
	Integer getInterval();

	void setInterval(Integer minute);

	@Description("Pollinginterval")
	@Default.Integer(60)
	Integer getPollingInterval();

	void setPollingInterval(Integer seconds);

	@Description("Path of the file to read from")
	ValueProvider<String> getInputFile();

	void setInputFile(ValueProvider<String> value);

	@Description("batch Size")
	ValueProvider<Integer> getBatchSize();

	void setBatchSize(ValueProvider<Integer> value);

	@Description("Number of Rows")
	ValueProvider<Integer> getNumberofRows();

	void setNumberofRows(ValueProvider<Integer> value);

	@Description("Table Spec")
	ValueProvider<String> getTableSpec();

	void setTableSpec(ValueProvider<String> value);

}
