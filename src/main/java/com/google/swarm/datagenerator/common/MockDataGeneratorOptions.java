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
