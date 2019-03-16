//************************************************************
// Copyright 2019 ANI Technologies Pvt. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//************************************************************
/**
 * 
 */
package com.olacabs.roadrunner.utils;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.olacabs.roadrunner.monitor.RoadRunnerMetricFactory;

public class RoadRunnerUtils {
	
	private static final RoadRunnerMetricFactory metrics = RoadRunnerMetricFactory.getInstance(); 

	private static Logger logger = LoggerFactory.getLogger(RoadRunnerUtils.class);

	private static Pattern p = Pattern.compile("[^a-zA-Z0-9_:]");
	
	public static String format(String name) {
		if (StringUtils.isEmpty(name)) {
			return "null";
		}
		Matcher m = p.matcher(name.trim());
		return m.replaceAll("_");
	}
	
	public static void printRRIcon() {
		printRRIcon(Constants.DEFAULT_RR_ICON_PATH);
	}

	/**
	 * Print the roadrunner icon on successfully booting an instance.
	 * @param iconSourcePath: Path to the roadrunner icon to be printed
	 */
	public static void printRRIcon(String iconSourcePath) {
		iconSourcePath = iconSourcePath == null ? Constants.DEFAULT_RR_ICON_PATH : iconSourcePath;
		ClassLoader classLoader = RoadRunnerUtils.class.getClassLoader();
		InputStream iconContentsStream = classLoader.getResourceAsStream(iconSourcePath);
		String rrIconAsText = null;
		try {
			rrIconAsText = readStreamContents(iconContentsStream);
		}
		catch (Exception err) {
			logger.error("Could not read icon from source (" + iconSourcePath + "). Error message: " + err.getMessage());
			rrIconAsText = Constants.DEFAULT_RR_ICON_AS_TEXT;
		}
		System.out.println(rrIconAsText);
	}

	/**
	 * Read stream contents into a string, and then close the stream.
	 * @param inputStream: The stream to read contents from
	 */
	private static String readStreamContents(InputStream inputStream) throws IOException {
		return readStreamContents(inputStream, Constants.DEFAULT_INPUT_STREAM_BUFFER_SIZE);
	}

	/**
	 * Read stream contents into a string, and then close the stream.
	 * @param inputStream: The stream to read contents from
	 * @param size: The size of the buffer for the internal BufferedInputStream
	 */
	private static String readStreamContents(InputStream inputStream, int size) throws IOException {
		try (BufferedInputStream bis = new BufferedInputStream(inputStream, size);
			 ByteArrayOutputStream buf = new ByteArrayOutputStream()) {
			int result = bis.read();
			while (result != -1) {
				buf.write((byte) result);
				result = bis.read();
			}
			return buf.toString("UTF-8");
		}
	}
	
	public static <T extends Object> void validateCompareValues(String fieldName, T... values) {
		boolean isValid = true;
		if(values == null || values.length == 0) {
			isValid = false;
		} else {
			for(T value : values) {
				if(value == null) {
					isValid = false;
				}
			}
		}
		
		if( ! isValid) {
			metrics.increment("searcher_invalid_values", 1);
			logger.error("Invalid values for fieldName : {}", fieldName);
			throw new IllegalArgumentException("Invalid values for fieldName : " + fieldName);
		}
	}

}
