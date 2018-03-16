package com.dataflowdeveloper.processors.convertjsontoddl;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Tags({ "convert-json-to-ddl" })
@CapabilityDescription("Create mostly complete SQL Table Create DDL from JSON")
@SeeAlso({})
@ReadsAttributes({
		@ReadsAttribute(attribute = "tableType", description = "What type of table:   hive, mysql, oracle, postgresql, phoenix") })
@WritesAttributes({ @WritesAttribute(attribute = "ddl", description = "SQL Create Table DDL as Text") })
public class JsonToDDLProcessor extends AbstractProcessor {

	private static final String FILENAME = "filename";
	public static final int PADDING_FACTOR = 12;
	private static final String TXT_CREATE_TABLE = "CREATE TABLE ";
	public static final String FIELD_TABLE_TYPE = "TABLE_TYPE";
	public static final String FIELD_TABLE_NAME = "TABLE_NAME";
	public static final String FIELD_DDL = "generatedddl";
	public static final String FIELD_SUCCESS = "success";
	public static final String FIELD_FAILURE = "failure";

	public static final String VALID_DATE_FORMAT = "yyyy-MM-dd";
	public static final String VALID_DATETIME_FORMAT = "mm/dd/yyyy HH:MM:SS";
	public static final String VALID_RFC822_DATETIME_FORMAT = "EEE, dd MMM yyyy HH:mm:ss Z";

	/**
	 * Potential extra parameters: DEFAULT_FIELD_SIZE, DEFAULT_TYPE, DEFAULT_NUMBER,
	 * PADDING_FACTOR, MINIMUM_FIELD_SIZE, MAXIMUM_FIELD_SIZE, DEFINITION_FILE,
	 * DEFAULT_VALUE, DEFAULT_NUMBER_TYPE date parsers, valid date format, valid
	 * datetime format, valid money/currency format, locale
	 */
	public static final PropertyDescriptor TABLE_TYPE = new PropertyDescriptor.Builder().name(FIELD_TABLE_TYPE)
			.displayName("tableType").description("Which table is it?  MySQL, Hive, Oracle, Postgresql, Phoenix?")
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(true).build();

	public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder().name(FIELD_TABLE_NAME)
			.displayName("tableName").description("The Name of the table, defaults to filename")
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR).expressionLanguageSupported(true).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name(FIELD_SUCCESS)
			.description("Successfully extract content.").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name(FIELD_FAILURE)
			.description("Failed to extract content.").build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	/**
	 * is a valid date
	 * 
	 * @param couldBeDate
	 * @return true/false on date
	 */
	public static boolean isValidDate(String couldBeDate) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(VALID_DATE_FORMAT);
		dateFormat.setLenient(true);
		try {
			dateFormat.parse(couldBeDate.trim());
		} catch (ParseException pe) {
			return false;
		}
		return true;
	}

	/**
	 * is a valid date
	 * 
	 * @param couldBeDate
	 * @return true/false on date
	 */
	public static boolean isValidDateTime(String couldBeDate) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(VALID_DATETIME_FORMAT);
		dateFormat.setLenient(true);
		try {
			dateFormat.parse(couldBeDate.trim());
		} catch (ParseException pe) {
			return false;
		}
		return true;
	}

	/**
	 * is a valid date
	 * 
	 * @param couldBeDate
	 * @return true/false on date
	 */
	public static boolean isValidRFC822DateTime(String couldBeDate) {
		SimpleDateFormat dateFormat = new SimpleDateFormat(VALID_RFC822_DATETIME_FORMAT);
		dateFormat.setLenient(true);
		try {
			dateFormat.parse(couldBeDate.trim());
		} catch (ParseException pe) {
			return false;
		}
		return true;
	}
	

	/**
	 * clean the name
	 * 
	 * @param dirtyName String
	 * @return cleanName String
	 */
	public static String cleanName(String dirtyName) {
		String cleanName = "";
		
		try { 
			cleanName = dirtyName.replaceFirst("[^A-Za-z]", "");
			cleanName = cleanName.replaceAll("[^A-Za-z0-9_]", "");
			cleanName = cleanName.replaceAll("\\:", "");
			cleanName = cleanName.replaceAll("\\.", "");				
		}
		catch(Throwable t) {
			
		}

		return cleanName;
	}
	
	/**
	 * parse JSON to a Table DDL see:
	 * https://github.com/apache/nifi/blob/19595863894a762b42fe7d6226835aacf207dcb6/nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/test/java/org/apache/nifi/processors/standard/TestConvertJSONToSQL.java
	 * https://github.com/apache/nifi/blob/19595863894a762b42fe7d6226835aacf207dcb6/nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/ConvertJSONToSQL.java
	 * 
	 * @param tableName
	 * @param tableType
	 * @param json
	 * @return String DDL SQL
	 */
	public String parse(String tableName, String json, String tableType) {
		JsonFactory factory = new JsonFactory();

		StringBuilder sql = new StringBuilder(256);
		sql.append(TXT_CREATE_TABLE).append(tableName).append(" ( ");

		ObjectMapper mapper = new ObjectMapper(factory);
		JsonNode rootNode = null;
		try {
			rootNode = mapper.readTree(json);
		} catch (Exception e) {
			getLogger().error("Unable to process Json parse " + e.getLocalizedMessage());
			return "";
		}

		// Could do some type stuff like
		// if oracle then STRING_TYPE= VARCHAR2
		// should be in a mapping file?

		if (rootNode != null) {
			Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();
			while (fieldsIterator.hasNext()) {
				Map.Entry<String, JsonNode> field = fieldsIterator.next();

				sql.append(cleanName(field.getKey()));

				if (field.getValue() != null) {

					if (field.getValue().canConvertToInt()) {
						sql.append(" INT, ");
					} else if (field.getValue().canConvertToLong()) {
						sql.append(" LONG, ");
					} else if (field.getValue().asText().length() <= 1) {
						sql.append(" CHAR(1), ");
					} else if (field.getValue().asText().equalsIgnoreCase("true") || field.getValue().asText().equalsIgnoreCase("false")) {
						sql.append(" BOOLEAN, ");
					} else {

						boolean isADate = false;

						try {
							Date date = Date.valueOf(field.getValue().asText());
							isADate = true;
							sql.append(" DATE, ");
						} catch (IllegalArgumentException e) {
							isADate = false;
						}

						if (!isADate) {

							DateValidator dateVal = new DateValidator();

							if (dateVal.validate(field.getValue().asText())) {
								sql.append(" DATETIME, ");
							} else if (isValidRFC822DateTime(field.getValue().asText())) {
								sql.append(" DATETIME, ");
							} else if (isValidDateTime(field.getValue().asText())) {
								sql.append(" DATETIME, ");
							} else if (isValidDate(field.getValue().asText())) {
								sql.append(" DATE, ");
							} else {
								sql.append(" VARCHAR(").append(field.getValue().asText().length() + PADDING_FACTOR)
										.append("), ");
							}
						}
					}
				} else {
					sql.append(" VARCHAR(50), ");
				}
			}
		}

		// end table
		sql.deleteCharAt(sql.length() - 2);
		sql.append(" ) ");
		return sql.toString();
	}

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(TABLE_TYPE);
		descriptors.add(TABLE_NAME);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return this.descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		return;
	}

	/**
	 * may want to clean attribute names?   clean field names in json
	 */
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			flowFile = session.create();
		}

		final String tableType = context.getProperty(FIELD_TABLE_TYPE).evaluateAttributeExpressions(flowFile).getValue();
		final String filename = flowFile.getAttribute(FILENAME);
		final String tableName = context.getProperty(FIELD_TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
		String selectedTableName = ((tableName != null) ? tableName.trim() : filename);
		
		final HashMap<String, String> attributes = new HashMap<String, String>();

		try {
			final AtomicReference<Boolean> wasError = new AtomicReference<>(false);

			flowFile = session.write(flowFile, new StreamCallback() {
				@Override
				public void process(InputStream inputStream, OutputStream outputStream) throws IOException {
					BufferedInputStream buffStream = new BufferedInputStream(inputStream);
					attributes.put(FIELD_DDL,
							parse(selectedTableName, IOUtils.toString(buffStream, "UTF8"), tableType));
					buffStream.close();
				}
			});

			if (wasError.get()) {
				session.transfer(flowFile, REL_FAILURE);
			} else {
				flowFile = session.putAllAttributes(flowFile, attributes);
				session.transfer(flowFile, REL_SUCCESS);
			}
			session.commit();
		} catch (final Throwable t) {
			getLogger().error("Unable to process Json JsonToDDLProcessor file " + t.getLocalizedMessage());
			getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, t });
			throw t;
		}
	}
}
