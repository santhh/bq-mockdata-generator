package com.google.swarm.datagenerator.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.util.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.Charsets;

public class Util {
	public static final Logger LOG = LoggerFactory.getLogger(Util.class);
	static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();
	static final Integer MAX_NUMBER = 9999;
	static final Integer MIN_NUMBER = 1;
	private static final String CHAR_LIST = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

	public static BufferedReader getReader(ReadableFile file) {

		BufferedReader br = null;
		try {
			ReadableByteChannel channel;
			channel = file.openSeekable();
			br = new BufferedReader(Channels.newReader(channel, Charsets.UTF_8.name()));

		} catch (IOException e) {
			e.printStackTrace();
		}
		return br;

	}

	public static int randomInt() {

		Random r = new Random();
		return r.ints(1, MIN_NUMBER, MAX_NUMBER).findFirst().getAsInt();

	}

	public static double randomFloat() {

		double random = new Random().nextDouble();
		return 1 + (random * (MAX_NUMBER - MIN_NUMBER));

	}

	public static String randomString(int length) {
		StringBuffer randStr = new StringBuffer(length);
		Random random = new Random();
		for (int i = 0; i < length; i++)
			randStr.append(CHAR_LIST.charAt(random.nextInt(CHAR_LIST.length())));
		return randStr.toString();
	}

	public static String randTimeStamp() {

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm[:ss[.SSSSSS]]");
		return ZonedDateTime.now().format(formatter);

	}

	public static String randDateTime() {

		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		return now.format(formatter);

	}

	public static String randDate() {

		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		return now.format(formatter);

	}

	public static String toJsonString(Object item) {
		if (item == null) {
			return null;
		}
		try {
			return JSON_FACTORY.toString(item);
		} catch (IOException e) {
			throw new RuntimeException(
					String.format("Cannot serialize %s to a JSON string.", item.getClass().getSimpleName()), e);
		}
	}

	public static String getPartitionTableName(String tableSpec) {

		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
		String partition = now.format(formatter);
		return String.format("%s$%s", tableSpec, partition);

	}

	public static BQObject createBqObject(Map<String, String> bqHeader, List<Map<String, String>> bqRows) {
		return BQObject.create(bqHeader, bqRows);

	}

	public static String checkValue(String value) {

		String result;
		switch (value) {
		case "STRING":
			result = Util.randomString(4);
			break;
		case "TIMESTAMP":
			result = Util.randTimeStamp();
			break;
		case "INTEGER":
			result = String.valueOf(Util.randomInt());
			break;
		case "INT64":
			result = String.valueOf(Util.randomInt());
			break;
		case "DATE":
			result = Util.randDate();
			break;
		case "DATETIME":
			result = Util.randDateTime();
			break;
		case "NUMERIC":
			result = String.valueOf(Util.randomFloat());
			break;
		case "FLOAT64":
			result = String.valueOf(Util.randomFloat());
			break;
		default:
			result = "***TYPE NOT SUPPORTED***";
			break;

		}
		//LOG.info("result:{}",result);
		return result;

	}

}
