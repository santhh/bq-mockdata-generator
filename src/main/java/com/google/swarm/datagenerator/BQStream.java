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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.swarm.datagenerator.common.BQDestination;
import com.google.swarm.datagenerator.common.BQStreamOptions;
import com.google.swarm.datagenerator.common.Util;


public class BQStream {
	private static final Logger LOG = LoggerFactory.getLogger(BQStream.class);
	public static Gson gson = new Gson();

	public static void main(String args[]) {
		BQStreamOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BQStreamOptions.class);

		run(options);

	}

	public static PipelineResult run(BQStreamOptions options) {
		Pipeline p = Pipeline.create(options);
		// pub sub
		PCollection<KV<String, TableRow>> pubSubDataCollection = p
				.apply("JSON Events", PubsubIO.readMessagesWithAttributes().fromSubscription(options.getSubTopic()))
				.apply("BQ Converts", MapElements.via(new SimpleFunction<PubsubMessage, KV<String, TableRow>>() {
					@SuppressWarnings("unchecked")
					@Override
					public KV<String, TableRow> apply(PubsubMessage json) {
						String eventType = json.getAttribute("event_type");
						if (eventType == null) {
							LOG.error("Message must contain event_type attribute {}", json.toString());
							throw new RuntimeException("Failed to parse message attribute " + json.toString());
						}
						String message = new String(json.getPayload(), StandardCharsets.UTF_8);
						TableRow row = Util.convertJsonToTableRow(message);

						Map<String, Object> map = new HashMap<String, Object>();

						List<TableCell> cells = new ArrayList<>();
						map = new HashMap<String, Object>();
						map = gson.fromJson(message, map.getClass());

						map.forEach((key, value) -> {
							cells.add(new TableCell().set(key, value));

						});

						row.setF(cells);

						return KV.of(eventType, row);
					}
				}));
		
		pubSubDataCollection.apply("Events Write",
				BigQueryIO.<KV<String, TableRow>>write()
						.to(new BQDestination(options.getDataSetId(), options.getProject()))
						.withFormatFunction(element -> {
							LOG.debug("BQ Row {}", element.getValue().getF());
							return element.getValue();
						}).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withoutValidation());

		return p.run();

	}

}
