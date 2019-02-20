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
