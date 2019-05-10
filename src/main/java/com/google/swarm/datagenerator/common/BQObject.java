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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class BQObject {

	private Map<String, String> bqHeader;

	private List<Map<String, String>> bqRows;

	public BQObject() {

		bqHeader = new HashMap<String, String>();
		bqRows = new ArrayList<Map<String, String>>();
	}

	public BQObject(Map<String, String> bqHeader, List<Map<String, String>> bqRows) {

		this.bqHeader = bqHeader;
		this.bqRows = bqRows;
	}

	public static BQObject create(Map<String, String> bqHeader, List<Map<String, String>> bqRows) {

		return new BQObject(bqHeader, bqRows);
	}

	public Map<String, String> getTableHeader() {

		return this.bqHeader;
	}

	public List<Map<String, String>> getTableData() {

		return this.bqRows;
	}

}
