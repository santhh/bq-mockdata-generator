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
package com.google.swarm.datagenerator;

import java.io.BufferedReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.swarm.datagenerator.common.BQObject;
import com.google.swarm.datagenerator.common.MockDataGeneratorOptions;
import com.google.swarm.datagenerator.common.Util;

public class MockDataGeneratorPipeline {

	public static final Logger LOG = LoggerFactory.getLogger(MockDataGeneratorPipeline.class);

	@SuppressWarnings("serial")
	public static class JSONSchemaReader extends DoFn<ReadableFile, KV<String, BQObject>> {

		private ValueProvider<Integer> batchSize;
		private ValueProvider<Integer> numoffRows;
		private List<Map<String, String>> bqRows;

		public JSONSchemaReader(ValueProvider<Integer> batchSize, ValueProvider<Integer> numofRows) {

			this.batchSize = batchSize;
			this.numoffRows = numofRows;
			this.bqRows = new ArrayList<Map<String, String>>();

		}

		@ProcessElement
		public void processElement(ProcessContext c, OffsetRangeTracker tracker) {

			for (long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {

				LOG.info("Started Restriction From: {}, To: {} ", tracker.currentRestriction().getFrom(),
						tracker.currentRestriction().getTo());

				BufferedReader br = Util.getReader(c.element());
				StringBuilder jsonSchema = new StringBuilder();
				String line = null;
				Map<String, String> headers = new HashMap<String, String>();
				Map<String, String> rows = new HashMap<String, String>();
				try {
					while ((line = br.readLine()) != null) {
						jsonSchema.append(line);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				JSONObject object = new JSONObject(jsonSchema.toString());

				String rootNode = JSONObject.getNames(object)[0];
				JSONArray nodes = object.getJSONArray(rootNode);

				for (int j = 0; j < nodes.length(); j++) {

					JSONObject node = nodes.getJSONObject(j);
					String[] keys = JSONObject.getNames(node);
					String headerKey = null;
					String headerValue = null;
					for (String key : keys) {

						if (key.equals("name")) {

							headerKey = (String) node.get(key);

						}

						if (key.equals("type")) {
							headerValue = (String) node.get(key);

						}
						// LOG.info("key: {}. Value: {}", key, value.toString());
					}

					headers.put(headerKey, headerValue);
				}

				// create data based on batch now
				int batchCount = 1;
				while (batchCount <= this.batchSize.get()) {

					Iterator<String> keySet = headers.keySet().iterator();
					while (keySet.hasNext()) {

						String key = keySet.next();
						String value = headers.get(key);
						rows.put(key, Util.checkValue(value));
					}
					bqRows.add(rows);
					batchCount = batchCount + 1;
				}

				BQObject output = Util.createBqObject(headers, bqRows);

				LOG.info("Completed Restriction From: {}, To: {} , Batach Count {}",
						tracker.currentRestriction().getFrom(), tracker.currentRestriction().getTo(), batchCount - 1);
				c.output(KV.of(String.valueOf(tracker.currentRestriction().getFrom()), output));
				bqRows.clear();
				headers.clear();
			} // end of restriction loop

		}

		@GetInitialRestriction
		public OffsetRange getInitialRestriction(ReadableFile dataFile) throws IOException, GeneralSecurityException {

			int totalSplit = this.numoffRows.get() / this.batchSize.get();
			LOG.info("****Total Number of Split**** {}", totalSplit);
			return new OffsetRange(1, totalSplit + 1);

		}

		@SplitRestriction
		public void splitRestriction(ReadableFile element, OffsetRange range, OutputReceiver<OffsetRange> out) {
			for (final OffsetRange p : range.split(1, 1)) {
				out.output(p);

			}
		}

		@NewTracker
		public OffsetRangeTracker newTracker(OffsetRange range) {
			return new OffsetRangeTracker(new OffsetRange(range.getFrom(), range.getTo()));

		}

	}

	@SuppressWarnings("serial")

	public static class FormatBQOutputData extends DoFn<KV<String, Iterable<BQObject>>, TableRow> {

		@ProcessElement
		public void processElement(ProcessContext c) throws IOException {

			Iterator<BQObject> bqObjects = c.element().getValue().iterator();

			while (bqObjects.hasNext()) {

				List<Map<String, String>> dataset = bqObjects.next().getTableData();

				LOG.info("Processing List Size: {}", dataset.size());

				dataset.forEach(data -> {

					Iterator<String> rows = data.keySet().iterator();
					TableRow bqRow = new TableRow();
					while (rows.hasNext()) {

						String key = rows.next();
						String value = data.get(key);
						bqRow.set(key, value);
					}
					c.output(bqRow);
				});

			}

		}
	}

	@SuppressWarnings("serial")

	public static class BQSchemaGenerator
			extends DoFn<KV<String, Iterable<BQObject>>, KV<String, Map<String, String>>> {

		private ValueProvider<String> tableSpec;

		public BQSchemaGenerator(ValueProvider<String> tableSpec) {
			this.tableSpec = tableSpec;

		}

		@ProcessElement
		public void processElement(ProcessContext c) {

			Iterable<BQObject> bqObjects = c.element().getValue();
			Map<String, String> headers = bqObjects.iterator().next().getTableHeader();

			c.output(KV.of(this.tableSpec.get(), headers));

		}

	}

	@SuppressWarnings("serial")
	public static void main(String[] args) throws IOException, GeneralSecurityException {

		MockDataGeneratorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(MockDataGeneratorOptions.class);

		Pipeline p = Pipeline.create(options);

		PCollection<KV<String, Iterable<BQObject>>> outputData = p
				.apply(FileIO.match().filepattern(options.getInputFile())
						.continuously(Duration.standardSeconds(options.getPollingInterval()), Watch.Growth.never()))
				.apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
				.apply("Json Schema File Reader",
						ParDo.of(new JSONSchemaReader(options.getBatchSize(), options.getNumberofRows())))
				.apply(Window
						.<KV<String, BQObject>>into(FixedWindows.of(Duration.standardSeconds(options.getInterval()))))

				.apply(GroupByKey.<String, BQObject>create());

		final PCollectionView<List<KV<String, Map<String, String>>>> schemasView = outputData
				.apply("AddToSchemaMap", ParDo.of(new BQSchemaGenerator(options.getTableSpec())))
				.apply("ViewSchemaAsMap", View.asList());

		outputData.apply("Format BQ Output Data", ParDo.of(new FormatBQOutputData()))
				.apply(BigQueryIO.writeTableRows().to(new DynamicDestinations<TableRow, String>() {
					@Override
					public String getDestination(ValueInSingleWindow<TableRow> element) {

						List<KV<String, Map<String, String>>> inputs = sideInput(schemasView);
						String tableSpec = inputs.get(0).getKey();
						TableRow row = element.getValue();

						String partitionDate = row.get("date_time").toString().replace("-", "").substring(0, 8);
						String destination = String.format("%s$%s", tableSpec, partitionDate);
						LOG.info("Destination: " + destination);
						return destination;

					}

					@Override
					public List<PCollectionView<?>> getSideInputs() {

						return ImmutableList.of(schemasView);

					}

					@Override
					public TableDestination getTable(String destination) {

						return new TableDestination(destination, null);

					}

					@Override
					public TableSchema getSchema(String destination) {

						List<KV<String, Map<String, String>>> inputs = sideInput(schemasView);

						Map<String, String> schemas = inputs.get(0).getValue();
						Iterator<String> headerList = schemas.keySet().iterator();
						List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
						TableSchema bqSchema = new TableSchema();
						while (headerList.hasNext()) {

							String key = headerList.next();
							String value = schemas.get(key);
							TableFieldSchema field = new TableFieldSchema();
							field.setName(key);
							field.setType(value);
							fields.add(field);

						}

						bqSchema.setFields(fields);

						LOG.info("Schema: {}, Destination: {}", Util.toJsonString(bqSchema), destination);

						return bqSchema;
					}
				}).withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

		p.run();

	}
}
