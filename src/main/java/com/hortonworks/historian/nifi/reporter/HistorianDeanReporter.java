/**
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
package com.hortonworks.historian.nifi.reporter;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasClient.EntityResult;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.htrace.fasterxml.jackson.core.JsonParseException;
import org.apache.htrace.fasterxml.jackson.databind.JsonMappingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.hortonworks.historian.model.HistorianDataTypes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Tags({"reporting", "atlas", "historian", "orchestration"})
@CapabilityDescription("Publishes Historian Tags from Druid to Apache Atlas, Exposes Druid Datasources as Hive Tables, Initiates Druid re-Indexing Jobs on Late Arriving Data.")
public class HistorianDeanReporter extends AbstractReportingTask {

	static final PropertyDescriptor HISTORIAN_TAG_DIMENSION = new PropertyDescriptor.Builder()
    		.name("Tag Dimension Name")
    		.description("The name of the dimension field in the Historian data source that contains tags (no spaces)")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("tag_dimension")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	static final PropertyDescriptor ATLAS_URL = new PropertyDescriptor.Builder()
            .name("Atlas URL")
            .description("The URL of the Atlas Server")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("http://localhost:21000")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    static final PropertyDescriptor NIFI_URL = new PropertyDescriptor.Builder()
            .name("Nifi URL")
            .description("The URL of the Nifi UI")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("http://localhost:9090")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();
    static final PropertyDescriptor HIVE_SERVER_CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("Hive Server Connection String")
            .description("The connection string for Hive Server that contains the database where Druid backed tables are managed.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("jdbc:hive2://localhost:10500/default")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor DRUID_BROKER_HTTP_ENDPOINT = new PropertyDescriptor.Builder()
    		.name("Druid Broker HTTP endpoint")
    		.description("Druid Broker HTTP endpoint")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("http://localhost:8082")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    static final PropertyDescriptor DRUID_METASTORE_CONNECTION_STRING = new PropertyDescriptor.Builder()
    		.name("Druid Meta Store Connection String")
    		.description("The connection string for the Druid Metastore that contains information about Druid's storage segments.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("jdbc:mysql://localhost:3306/druid_meta")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    private int timesTriggered = 0;
    private AtlasClient atlasClient;
    
    private Double atlasVersion = 0.0;
    private String encoding = "YWRtaW46YWRtaW4=";
    private String DEFAULT_ADMIN_USER = "admin";
    private String DEFAULT_ADMIN_PASS = "admin";
    private String atlasUrl;
    private String nifiUrl;
    private String druidBrokerUrl;
    private String hiveServerUri;
    private String[] basicAuth = {DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASS};
    
    private DataTypes.MapType STRING_MAP_TYPE = new DataTypes.MapType(DataTypes.STRING_TYPE, DataTypes.STRING_TYPE);
    private Map<String,Object> entityMap = new HashMap<String,Object>();
    
    private String NAME = "name";
    private String SOURCE = "source";
    private String DESTINATION = "destination";
    private String PROPERTIES = "parameters";
    private String TAG_DIMENSION_NAME = "tag_dimension";
    
    private Map<String,Map<String, Object>> dataSourceDetails = new HashMap<String,Map<String,Object>>();
    private Map<String, EnumTypeDefinition> enumTypeDefinitionMap = new HashMap<String, EnumTypeDefinition>();
	private Map<String, StructTypeDefinition> structTypeDefinitionMap = new HashMap<String, StructTypeDefinition>();
	private Map<String, HierarchicalTypeDefinition<ClassType>> classTypeDefinitions = new HashMap<String, HierarchicalTypeDefinition<ClassType>>();
	private List<Referenceable> inputs;
	private List<Referenceable> outputs;
	
	Connection hiveConnection;
	
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HISTORIAN_TAG_DIMENSION);
        properties.add(ATLAS_URL);
        properties.add(NIFI_URL);
        properties.add(HIVE_SERVER_CONNECTION_STRING);
        properties.add(DRUID_BROKER_HTTP_ENDPOINT);
        //properties.add(DRUID_METASTORE_CONNECTION_STRING);
        return properties;
    }
    
    public void initialize(ConfigurationContext reportingConfig){}
    
    @Override
    public void onTrigger(ReportingContext reportingContext) {
    	// create the Atlas client if we don't have one
    	/*
    	Properties props = System.getProperties();
        props.setProperty("atlas.conf", "/usr/hdp/current/atlas-client/conf");
        getLogger().info("***************** atlas.conf has been set to: " + props.getProperty("atlas.conf"));
    	*/
        inputs = new ArrayList<Referenceable>();
    	outputs = new ArrayList<Referenceable>();
        //EventAccess eventAccess = reportingContext.getEventAccess();
        //int pageSize = reportingContext.getProperty(ACTION_PAGE_SIZE).asInteger();
        atlasUrl = reportingContext.getProperty(ATLAS_URL).getValue();
        nifiUrl = reportingContext.getProperty(NIFI_URL).getValue();
        druidBrokerUrl = reportingContext.getProperty(DRUID_BROKER_HTTP_ENDPOINT).getValue();
        hiveServerUri = reportingContext.getProperty(HIVE_SERVER_CONNECTION_STRING).getValue();
        TAG_DIMENSION_NAME = reportingContext.getProperty(HISTORIAN_TAG_DIMENSION).getValue();
        //druidMetaUri = reportingContext.getProperty(DRUID_METASTORE_CONNECTION_STRING).getValue();
        String[] atlasURL = {atlasUrl};
		
    	if (atlasClient == null) {
            getLogger().info("Creating new Atlas client for {}", new Object[] {atlasUrl});
            atlasClient = new AtlasClient(atlasURL, basicAuth);
        }
    	
    	if(atlasVersion == 0.0){
        	atlasVersion = Double.valueOf(getAtlasVersion(atlasUrl + "/api/atlas/admin/version", basicAuth));
        	getLogger().info("********************* Atlas Version is: " + atlasVersion);
    	}
    	
    	getLogger().info("********************* Number of Reports Sent: " + timesTriggered);
        if(timesTriggered == 0){
        	String hiveUsername = "hive";
		    String hivePassword = "hive";
		    
        	try {
        		getLogger().info("********************* Establishing Connection to Hive Server...");
        		Class.forName("org.apache.hive.jdbc.HiveDriver");
        		hiveConnection = DriverManager.getConnection(hiveServerUri, hiveUsername, hivePassword);
				
        		getLogger().info("********************* Create Business Taxonomy Terms...");
        		String termPath = "/Catalog/terms/Unassigned";
        		String termDefinition = "{\"name\":\"Unassigned\",\"description\":\"\"}";
        		createBusinessTerm(termPath, termDefinition);
        		
				getLogger().info("********************* Checking if data model has been created...");
				/*
				try {
					atlasClient.getType(HistorianDataTypes.TAG_DIMENSION.getName());
					getLogger().info("********************* Trait: " + HistorianDataTypes.TAG_DIMENSION.getName() + " is already present");
				} catch (AtlasServiceException e) {
					getLogger().info("***************** Creating " + HistorianDataTypes.TAG_DIMENSION.getName() + " Trait...");
					atlasClient.createTraitType(HistorianDataTypes.TAG_DIMENSION.getName());
				}*/
				String historianDataModelJSON = generateHistorianDataModel();
        		getLogger().info("***************** Historian Data Model as JSON = " + historianDataModelJSON);
        		//atlasClient.createType(historianDataModelJSON);
				getLogger().info("********************* Created Types: " + atlasClient.createType(historianDataModelJSON));
				
				updateHiveColumnClassAttributes();
				
			} catch (AtlasServiceException e) {
				e.printStackTrace();
			} catch (AtlasException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }
        timesTriggered++;
        
        getLogger().info("********************* Looking for Druid Datasources to expose as Hive Tables or update with new information...");
        Iterator<String> resultIterator = getDruidDataSourceList().iterator();
		while(resultIterator.hasNext()){
			String dataSource = resultIterator.next();
			dataSourceDetails.put(dataSource, getDruidDataSourceDetails(dataSource));
			
			getLogger().info("********************* Exposing Druid Data Source: " + dataSource);
			exposeDruidDataSourceAsHiveTable(dataSource);
			
			getLogger().info("********************* Update Atlas Hive Tables and Column for Druid Data Source: " + dataSource);
			updateDataSourceHiveColumnAttributes(dataSource);
		}

		getLogger().info("********************* Done...");
		
    }
    
    public void updateDataSourceHiveColumnAttributes(String dataSource){
    	String dslQuery = "hive_table where name = '"+dataSource+"'";
		
		try {
			JSONArray results = atlasClient.searchByDSL(dslQuery,1,-1);
			Map<String,Object> referenceableJSON = new ObjectMapper().readValue(results.get(0).toString(), Map.class);
			//System.out.println(referenceableJSON);
			String tableId = ((Map)referenceableJSON.get("$id$")).get("id").toString();
		
			Referenceable tableRef = atlasClient.getEntity(tableId);
			List<Referenceable> columnRefs = (List<Referenceable>) tableRef.getValuesMap().get("columns");
		
			Iterator<Referenceable> columnsIterator = columnRefs.iterator();
			getLogger().info("********************* Discovering Tags and Updating Hive Columns in Atlas: " + dataSource);
			while(columnsIterator.hasNext()){
				Referenceable columnRef = columnsIterator.next();
				getLogger().debug("********** Column Referencebales: " + columnRef);
				String columnName = columnRef.getValuesMap().get("name").toString();
				String granularity = deserializeDataSourceGranularity(dataSource);
				String column_function = deserializeDataSourceColumnType(dataSource,columnName);
				String column_type = column_function.equalsIgnoreCase("time") || column_function.equalsIgnoreCase("dimension") ?  column_function : "metric"; 
				columnRef.set("granularity", granularity);
				columnRef.set("column_type", column_type);
				columnRef.set("column_function", column_function);
				getLogger().info("********************* Updating Hive Column: " + columnName);
				if(columnName.equalsIgnoreCase(TAG_DIMENSION_NAME) && granularity.equalsIgnoreCase("NONE")){	
					getLogger().info("********************* This Column is a Tag_Dimension field, discovering Historian Tags...");
					EntityResult result = atlasClient.updateEntities(discoverNewTags(tableRef,columnRef));
					Iterator<String> resultIterator = result.getCreatedEntities().iterator();
					getLogger().info("********************* Adding Unassigned Term to Historian Tag GUIDs... "+result.getCreatedEntities().toString());
					while(resultIterator.hasNext()){
						String currentEntity = resultIterator.next();
						getLogger().info("********************* Calling Atlas with URL: "+atlasUrl+"/api/atlas/v1/entities/"+currentEntity+"/tags/Catalog.Unassigned");
						postJSONToUrlAuth(atlasUrl+"/api/atlas/v1/entities/"+currentEntity+"/tags/Catalog.Unassigned" ,basicAuth,"{}");
					}
				}else{
					atlasClient.updateEntities(columnRef);
				}
				getLogger().debug("********** JSON Payload for Column/Tag Update: " + InstanceSerialization.toJson(columnRef, true));
			}
    	}catch (AtlasServiceException e) {
			e.printStackTrace();
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
    }
    
    public String deserializeDataSourceColumnType(String dataSource, String column){
		Map<String,Object>dataSourceMap = dataSourceDetails.get(dataSource);
		Map<String,Object>columnMap = (Map)dataSourceMap.get("columns");
		Map<String,Object>aggregatorMap = (Map)dataSourceMap.get("aggregators");
		
		String columnType = null;
		if(column.equalsIgnoreCase("__time")){
			columnType = "time";
			getLogger().debug("********** Column: " + column + ":" + columnType);
		}else if(((Map)columnMap.get(column)).get("cardinality") != null){
			columnType = "dimension";
			getLogger().debug("********** Column: " + column + ":" + columnType);
		}else{
			for(String aggregator:aggregatorMap.keySet()){
				String metricColumn = ((Map)aggregatorMap.get(aggregator)).get("fieldName").toString();
				if(metricColumn.equalsIgnoreCase(column)){
					String storedType = ((Map)aggregatorMap.get(aggregator)).get("type").toString();  
					if(metricColumn.equalsIgnoreCase("count")){
						columnType = "count";
					}else if(storedType.contains("Sum")){
						columnType = "SUM";
					}else if(storedType.contains("Min")){
						columnType = "MIN";
					}else if(storedType.contains("Max")){
						columnType = "MAX";
					}else if(storedType.contains("Avg")){
						columnType = "AVG";
					}
					getLogger().debug("********** Column: " + metricColumn + ":" +columnType);
					break;
				}
			}
		}
    	return columnType;
    }
	
	public String deserializeDataSourceGranularity(String dataSource){
    	Map<String,Object> granularityMap = dataSourceDetails.get(dataSource);
    	getLogger().debug("********** granularityMap: " + granularityMap);
    	String granularityType = ((HashMap)granularityMap.get("queryGranularity")).get("type").toString();
		String granularity = "";
		if(granularityType.equalsIgnoreCase("none")){
			granularity = "NONE";
		}else if(granularityType.equalsIgnoreCase("all")){
				granularity = "ALL";	
		}else{
			String granularityDuration = ((HashMap)granularityMap.get("queryGranularity")).get("duration").toString();
			switch(granularityDuration){
				case "1000":  
					granularity = "SECOND";
					break;
				case "60000":
					granularity = "MINUTE";
					break;
				case "3600000":
					granularity = "HOUR";
					break;
				case "86400000":
					granularity = "DAY";
					break;
			}
		}
    	return granularity;
    }
	
	private JSONObject createBusinessTerm(String taxonomyPath, String termDefinition){
		String atlasTaxonomyUrl = atlasUrl + "/api/atlas/v1/taxonomies" + taxonomyPath;
		JSONObject json = null;
		try {
			json = postJSONToUrlAuth(atlasTaxonomyUrl, basicAuth, termDefinition);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return json;
	}
	
	private Map<String, Object> getDruidDataSourceDetails(String dataSource) {
    	String druidSegmentUrl = druidBrokerUrl + "/druid/v2";
    	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String currentDate = dateFormat.format(new Date()).toString();
		String dateBefore = dateFormat.format(new Date(new Date().getTime() - (30 * 24 * 3600 * 1000L))).toString();
		
		String payload = "{\"queryType\":\"segmentMetadata\","
						+ "\"dataSource\":\""+dataSource+"\","
						//+ "\"intervals\":[\""+dateBefore+"/"+currentDate+"\"],"
						+ "\"analysisTypes\":[\"queryGranularity\",\"aggregators\",\"rollup\"],"
						+ "\"merge\":\"true\" "
						+ "}";
		List<Map<String,Object>> result = null;
		JSONArray druidSegmentsJSON;
		try {
			getLogger().debug("************************ Url: " + druidSegmentUrl);
			getLogger().debug("************************ Sending: " + payload);
			druidSegmentsJSON = readJSONArrayFromUrlAuthPOST(druidSegmentUrl, basicAuth, payload);
			getLogger().debug("************************ Response from Druid: " + druidSegmentsJSON);
	    	result = new ObjectMapper().readValue(druidSegmentsJSON.toString(), List.class);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
    	Map<String,Object> dataSourceDetailsMap = (Map)result.get(0);
    	
    	return dataSourceDetailsMap; 
	}

	public List<String> getDruidDataSourceList(){
		String druidDataSourceUrl = druidBrokerUrl + "/druid/v2/datasources";
		List<String> result = null;
    	JSONArray druidDataSourceJSON;
		try {
			getLogger().info("********************* Getting List of Druid Datasources from API: " + druidDataSourceUrl);
			druidDataSourceJSON = readJSONArrayFromUrlAuth(druidDataSourceUrl, basicAuth);
			getLogger().debug("************************ Response from Druid: " + druidDataSourceJSON);
	    	result = new ObjectMapper().readValue(druidDataSourceJSON.toString(), List.class);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
    	
    	return result;
    }
	
	public List<Referenceable> discoverNewTags(Referenceable tableRef, Referenceable columnRef){
		String sqlString = null;
		List<Referenceable> tagReferenceableList = new ArrayList<Referenceable>();
		List<Id> tagIdList = new ArrayList<Id>();
		try {
			Id currColumnRefId = columnRef.getId();
			String currColumnName = columnRef.getValuesMap().get("name").toString();
			String currTableName = tableRef.get("name").toString();
			sqlString = " SELECT `"+currColumnName+"`, COUNT(`"+currColumnName+"`)"
									+ " FROM "+currTableName+" "
									+ " GROUP BY `"+currColumnName+"`";
				
			getLogger().debug("********************* Executing Hive Query: " + sqlString);
			ResultSet result = hiveConnection.createStatement().executeQuery(sqlString);
			while(result.next()){
				String currGranularity = deserializeDataSourceGranularity(currTableName);
				Referenceable currTagReferenceable = new Referenceable(HistorianDataTypes.HISTORIAN_TAG.getName());
				currTagReferenceable.set("name",result.getString(currColumnName));
				currTagReferenceable.set("qualifiedName",currTableName+"."+currColumnName+"."+result.getString(currColumnName));
				currTagReferenceable.set("parent_column", currColumnRefId);
				currTagReferenceable.set("granularity", currGranularity);
				getLogger().debug("********************* New Tag Entity: " + InstanceSerialization.toJson(currTagReferenceable,true));
				tagReferenceableList.add(currTagReferenceable);	
				tagIdList.add(currTagReferenceable.getId());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		columnRef.set("historian_tags", tagIdList);
		tagReferenceableList.add(columnRef);
		return tagReferenceableList;
	}
	
	public List<Referenceable> discoverNewTags(JSONArray results){
		String sqlString = null;
		List<HashMap> referenceablesJSON = null;
		List<Referenceable> tagReferenceableList = new ArrayList<Referenceable>();
		try {
			referenceablesJSON = new ObjectMapper().readValue(results.toString(), List.class);
			Iterator<HashMap> refIterator = referenceablesJSON.iterator();
			while(refIterator.hasNext()){
				HashMap currReferenceable = refIterator.next();
				String currColumnId = ((HashMap)currReferenceable.get("$id$")).get("id").toString();
				String currColumnVersion = ((HashMap)currReferenceable.get("$id$")).get("version").toString();
				String currColumnType = ((HashMap)currReferenceable.get("$id$")).get("$typeName$").toString();
				String currColumnState = ((HashMap)currReferenceable.get("$id$")).get("state").toString();
				String currColumnName = currReferenceable.get("name").toString();
				Id currColumnRefId = new Id(currColumnId,Integer.valueOf(currColumnVersion),currColumnType,currColumnState);
				
				String tableId = ((HashMap)currReferenceable.get("table")).get("id").toString(); 
				Referenceable currTable = atlasClient.getEntity(tableId);
				String currTableName = currTable.get("name").toString();
				sqlString = " SELECT `"+currColumnName+"`, COUNT(`"+currColumnName+"`)"
									+ " FROM "+currTableName+" "
									+ " GROUP BY `"+currColumnName+"`";
				
				System.out.println("********************* Executing Hive Query: " + sqlString);
				ResultSet result = hiveConnection.createStatement().executeQuery(sqlString);
				while(result.next()){
					String currGranularity = deserializeDataSourceGranularity(currTableName);
					Referenceable currTagReferenceable = new Referenceable(HistorianDataTypes.HISTORIAN_TAG.getName());
					currTagReferenceable.set("name",result.getString(currColumnName)+"_"+currGranularity);
					currTagReferenceable.set("qualifiedName",currTableName+"."+currColumnName+"."+result.getString(currColumnName));
					currTagReferenceable.set("parent_column", currColumnRefId);
					currTagReferenceable.set("granularity", currGranularity);
					System.out.println("********************* New Tag Entity: " + InstanceSerialization.toJson(currTagReferenceable,true));
					tagReferenceableList.add(currTagReferenceable);	
				}
			}
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return tagReferenceableList;
	}
    
    public void exposeDruidDataSourceAsHiveTable(String dataSource){
	    String hiveTableName = "";
	    try {
	    	hiveTableName = dataSource;
	    	dataSourceDetails.put(hiveTableName, getDruidDataSourceDetails(hiveTableName));
	    	getLogger().info("********************* Attempting to create Hive Table from Druid Data Source: " + hiveTableName);
	    	hiveConnection.createStatement().execute("CREATE EXTERNAL TABLE IF NOT EXISTS " + hiveTableName + " "
		    				+ "STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler' "
		    				+ "TBLPROPERTIES (\"druid.datasource\" = \"" + hiveTableName + "\")");
	    }catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
            e.printStackTrace();
        }
    }
    
	private JSONArray readJSONArrayFromUrlAuthPOST(String urlString, String[] basicAuth, String payload) throws IOException, JSONException {
		String userPassString = basicAuth[0]+":"+basicAuth[1];
		JSONObject json = null;
		JSONArray jsonArray = null;
		try {
            URL url = new URL (urlString);
            //Base64.encodeBase64String(userPassString.getBytes());
            
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic " + encoding);
            connection.setRequestProperty("Content-Type", "application/json");
            
            OutputStream os = connection.getOutputStream();
    		os.write(payload.getBytes());
    		os.flush();
            
            if (connection.getResponseCode() != 200) {
    			throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode() + " : " + connection.getResponseMessage());
    		}
            
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	jsonArray = new JSONArray(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return jsonArray;
    }
	
	private void updateHiveColumnClassAttributes() throws AtlasException {
        final String typeName = "hive_column";

		try {
			TypesDef hiveColumnType = atlasClient.getType(typeName);
			AttributeDefinition[] attributes = hiveColumnType.classTypesAsJavaList().get(0).attributeDefinitions;
			ImmutableSet<String> superTypes = hiveColumnType.classTypesAsJavaList().get(0).superTypes;
			AttributeDefinition[] attributeDefinitions = Arrays.copyOf(attributes, attributes.length+4);
			attributeDefinitions[attributes.length] = new AttributeDefinition("column_type", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null);
			attributeDefinitions[attributes.length+1] = new AttributeDefinition("column_function", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null);
			attributeDefinitions[attributes.length+2] = new AttributeDefinition("granularity", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null);
			attributeDefinitions[attributes.length+3] = new AttributeDefinition("historian_tags", DataTypes.arrayTypeName(HistorianDataTypes.HISTORIAN_TAG.getName()), Multiplicity.OPTIONAL, false, null);
			
			HierarchicalTypeDefinition<ClassType> updateClass = TypesUtil.createClassTypeDef(typeName, superTypes, attributeDefinitions);
			getLogger().info("********** Updating " + typeName + " definition: " + TypesSerialization.toJson(updateClass,false));
			atlasClient.updateType(TypesSerialization.toJson(updateClass,false));
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		}
        //addClassTypeUpdateDefinition(typeName, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        getLogger().info("Updated definition for " + typeName);
    }
	
	public String generateHistorianDataModel() throws AtlasException {
    	TypesDef typesDef;
		String historianDataModelJSON;
		System.out.println("***************** generate data model method call...");
    	try {
			atlasClient.getType(HistorianDataTypes.HISTORIAN_ASSET.getName());
			getLogger().info("********************* Historian Atlas Type: " + HistorianDataTypes.HISTORIAN_ASSET.getName() + " is already present");
		} catch (AtlasServiceException e) {
			System.out.println("***************** create asset class...");
			createAssetClass();
		}
		
		try {
			atlasClient.getType(HistorianDataTypes.HISTORIAN_TAG.getName());
			getLogger().info("********************* Historian Atlas Type: " + HistorianDataTypes.HISTORIAN_TAG.getName() + " is already present");
		} catch (AtlasServiceException e) {
			System.out.println("***************** create tag class...");
			createTagClass();
		}
		
		try {
			atlasClient.getType(HistorianDataTypes.HISTORIAN_TAG_ATTRIBUTE.getName());
			getLogger().info("********************* Historian Atlas Type: " + HistorianDataTypes.HISTORIAN_TAG.getName() + " is already present");
		} catch (AtlasServiceException e) {
			System.out.println("***************** create tag attribute class...");
			createTagAttributeClass();
		}
		
		typesDef = TypesUtil.getTypesDef(
				getEnumTypeDefinitions(), 	//Enums 
				getStructTypeDefinitions(), //Struct 
				getTraitTypeDefinitions(), 	//Traits 
				ImmutableList.copyOf(classTypeDefinitions.values()));
		
		historianDataModelJSON = TypesSerialization.toJson(typesDef);
		
		getLogger().info("Submitting Types Definition: " + historianDataModelJSON);
		getLogger().info("Generating the Historian Data Model....");
		return historianDataModelJSON;
    }

    private void createAssetClass() throws AtlasException {
        final String typeName = HistorianDataTypes.HISTORIAN_ASSET.getName();
        
        final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
        		new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
        		new AttributeDefinition("parent_assets", DataTypes.arrayTypeName(HistorianDataTypes.HISTORIAN_ASSET.getName()), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("child_assets", DataTypes.arrayTypeName(HistorianDataTypes.HISTORIAN_ASSET.getName()), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("historian_tags", DataTypes.arrayTypeName(HistorianDataTypes.HISTORIAN_TAG.getName()), Multiplicity.OPTIONAL, true, null)
        };
        
        addClassTypeDefinition(typeName, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        getLogger().info("Created definition for " + typeName);
    }

    private void createTagClass() throws  AtlasException {
        final String typeName = HistorianDataTypes.HISTORIAN_TAG.getName();

        final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
        		new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
        		new AttributeDefinition("granularity", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
        		new AttributeDefinition("parent_column", "hive_column", Multiplicity.OPTIONAL, false, null),
        		new AttributeDefinition("parent_asset", HistorianDataTypes.HISTORIAN_ASSET.getName(), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition("tags_attributes", DataTypes.arrayTypeName(HistorianDataTypes.HISTORIAN_TAG_ATTRIBUTE.getName()), Multiplicity.OPTIONAL, true, null)
        };

        addClassTypeDefinition(typeName, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        getLogger().info("Created definition for " + typeName);
    }

    private void createTagAttributeClass() throws AtlasException {
        final String typeName = HistorianDataTypes.HISTORIAN_TAG_ATTRIBUTE.getName();

        final AttributeDefinition[] attributeDefinitions = new AttributeDefinition[] {
        		new AttributeDefinition(NAME, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
        		new AttributeDefinition("associated_tags", DataTypes.arrayTypeName(HistorianDataTypes.HISTORIAN_TAG.getName()), Multiplicity.OPTIONAL, false, null),
                new AttributeDefinition(PROPERTIES, STRING_MAP_TYPE.getName(), Multiplicity.OPTIONAL, false, null)
        };

        addClassTypeDefinition(typeName, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE), attributeDefinitions);
        getLogger().info("Created definition for " + typeName);
    }

    private void addClassTypeDefinition(String typeName, ImmutableSet<String> superTypes, AttributeDefinition[] attributeDefinitions) {
        final HierarchicalTypeDefinition<ClassType> definition =
                new HierarchicalTypeDefinition<>(ClassType.class, typeName, null, superTypes, attributeDefinitions);

        classTypeDefinitions.put(typeName, definition);
    }
    
    public TypesDef getTypesDef() {
        return TypesUtil.getTypesDef(getEnumTypeDefinitions(), getStructTypeDefinitions(), getTraitTypeDefinitions(), getClassTypeDefinitions());
    }

    public String getDataModelAsJSON() {
        return TypesSerialization.toJson(getTypesDef());
    }

    public ImmutableList<EnumTypeDefinition> getEnumTypeDefinitions() {
        return ImmutableList.copyOf(enumTypeDefinitionMap.values());
    }

    public ImmutableList<StructTypeDefinition> getStructTypeDefinitions() {
        return ImmutableList.copyOf(structTypeDefinitionMap.values());
    }

    public ImmutableList<HierarchicalTypeDefinition<ClassType>> getClassTypeDefinitions() {
        return ImmutableList.copyOf(classTypeDefinitions.values());
    }

    public ImmutableList<HierarchicalTypeDefinition<TraitType>> getTraitTypeDefinitions() {
        return ImmutableList.of();
    }
	
	private String getAtlasVersion(String urlString, String[] basicAuth){
		getLogger().info("************************ Getting Atlas Version from: " + urlString);
		JSONObject json = null;
		String versionValue = null;
        try{
        	json = readJSONFromUrlAuth(urlString, basicAuth);
        	getLogger().info("************************ Response from Atlas: " + json);
        	versionValue = json.getString("Version");
        } catch (Exception e) {
            e.printStackTrace();
        }
		return versionValue.substring(0,3);
	}
	
	private HashMap<String, Object> getProcessorConfig(String processorId, String urlString, String[] basicAuth){
		String processorResourceUri = "/nifi-api/processors/";
		String nifiProcessorUrl = urlString+processorResourceUri+processorId;
		System.out.println("************************ Getting Nifi Processor from: " + nifiProcessorUrl);
		JSONObject json = null;
		JSONObject nifiComponentJSON = null;
		HashMap<String,Object> result = null;
        try{
        	json = readJSONFromUrlAuth(nifiProcessorUrl, basicAuth);
        	System.out.println("************************ Response from Nifi: " + json);
        	nifiComponentJSON = json.getJSONObject("component").getJSONObject("config").getJSONObject("properties");
        	result = new ObjectMapper().readValue(nifiComponentJSON.toString(), HashMap.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
       
		return result;
	}
	
	private JSONObject readJSONFromUrlAuth(String urlString, String[] basicAuth) throws IOException, JSONException {
		String userPassString = basicAuth[0]+":"+basicAuth[1];
		JSONObject json = null;
		try {
            URL url = new URL (urlString);
            //Base64.encodeBase64String(userPassString.getBytes());

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", "Basic " + encoding);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	json = new JSONObject(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return json;
    }
	
	private JSONArray readJSONArrayFromUrlAuth(String urlString, String[] basicAuth) throws IOException, JSONException {
		String userPassString = basicAuth[0]+":"+basicAuth[1];
		JSONObject json = null;
		JSONArray jsonArray = null;
		try {
            URL url = new URL (urlString);
            //Base64.encodeBase64String(userPassString.getBytes());

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setDoOutput(true);
            connection.setRequestProperty  ("Authorization", "Basic " + encoding);
            InputStream content = (InputStream)connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(content, Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	jsonArray = new JSONArray(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return jsonArray;
    }
	
	private JSONObject postJSONToUrlAuth(String urlString, String[] basicAuth, String payload) throws IOException, JSONException {
		String userPassString = basicAuth[0]+":"+basicAuth[1];
		JSONObject json = null;
		try {
            URL url = new URL (urlString);
            //Base64.encodeBase64String(userPassString.getBytes());

            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic " + encoding);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("X-XSRF-HEADER","User");
            
            //System.out.println("To String: " + convertPOJOToJSON(historianEvent));
            
            OutputStream os = connection.getOutputStream();
    		os.write(payload.getBytes());
    		os.flush();
            
            if (connection.getResponseCode() != 200 || connection.getResponseCode() != 201) {
    			throw new RuntimeException("Failed : HTTP error code : " + connection.getResponseCode());
    		}
            BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
  	      	String jsonText = readAll(rd);
  	      	json = new JSONObject(jsonText);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return json;
    }
	
	private String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	}
	
	public void registerHistorianMetaData(){
		System.out.println("***************** Creating Meta Data Entities...");
		
		Referenceable tag_rpm_a = new Referenceable("historian_tag");
		tag_rpm_a.set(AtlasClient.NAME, "rpm_truck_a");
		tag_rpm_a.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_tag.rpm_truck_a");
		tag_rpm_a.set(AtlasClient.OWNER, "");
		
		Referenceable tag_rpm_b = new Referenceable("historian_tag");
		tag_rpm_b.set(AtlasClient.NAME, "rpm_truck_b");
		tag_rpm_b.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_tag.rpm_truck_b");
		tag_rpm_b.set(AtlasClient.OWNER, "");
		
		Referenceable tag_mpg_a = new Referenceable("historian_tag");
		tag_mpg_a.set(AtlasClient.NAME, "mpg_truck_a");
		tag_mpg_a.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_tag.mpg_truck_a");
		tag_mpg_a.set(AtlasClient.OWNER, "");
		
		Referenceable tag_mpg_b = new Referenceable("historian_tag");
		tag_mpg_b.set(AtlasClient.NAME, "mpg_truck_b");
		tag_mpg_b.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_tag.mpg_truck_b");
		tag_mpg_b.set(AtlasClient.OWNER, "");
		
		Referenceable truck_a = new Referenceable("historian_asset");
		truck_a.set(AtlasClient.NAME, "truck_a");
		truck_a.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_asset.truck_a");
		truck_a.set(AtlasClient.OWNER, "");
		
		Referenceable truck_b = new Referenceable("historian_asset");
		truck_b.set(AtlasClient.NAME, "truck_b");
		truck_b.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_asset.truck_b");
		truck_b.set(AtlasClient.OWNER, "");
		
		Referenceable mine_a = new Referenceable("historian_asset");
		mine_a.set(AtlasClient.NAME, "mine_a");
		mine_a.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_asset.mine_a");
		mine_a.set(AtlasClient.OWNER, "");
		
    	Referenceable mine_b = new Referenceable("historian_asset");
		mine_b.set(AtlasClient.NAME, "mine_b");
		mine_b.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_asset.mine_b");
		mine_b.set(AtlasClient.OWNER, "");
		
		 
		try {
			List<String> response = atlasClient.createEntity(tag_mpg_a);
			System.out.println(response);
			if(response.size() > 0){
				String tag_mpg_a_guid = response.get(0);
				tag_mpg_a = atlasClient.getEntity(tag_mpg_a_guid);
				Id tag_mpg_a_id = tag_mpg_a.getId();
				entityMap.put("tag_mpg_a", tag_mpg_a);
				entityMap.put("tag_mpg_a_id", tag_mpg_a_id);
				System.out.println(entityMap);
			}else{
				System.out.println("***************** tag_mpg_a already exists");
			}
			System.out.println(entityMap);
			response = atlasClient.createEntity(tag_rpm_a);
			System.out.println(response);
			if(response.size() > 0){
				String tag_rpm_a_guid = response.get(0);
				tag_rpm_a = atlasClient.getEntity(tag_rpm_a_guid);
				Id tag_rpm_a_id = tag_rpm_a.getId();
				entityMap.put("tag_rpm_a", tag_rpm_a);
				entityMap.put("tag_rpm_a_id", tag_rpm_a_id);
			}else{
				System.out.println("***************** tag_rpm_a already exists");
			}
			System.out.println(entityMap);
			response = atlasClient.createEntity(tag_mpg_b);
			System.out.println(response);
			if(response.size() > 0){
				String tag_mpg_b_guid = response.get(0);
				tag_mpg_b = atlasClient.getEntity(tag_mpg_b_guid);
				Id tag_mpg_b_id = tag_mpg_b.getId();
				entityMap.put("tag_mpg_b", tag_mpg_b);
				entityMap.put("tag_mpg_b_id", tag_mpg_b_id);
			}else{
				System.out.println("***************** tag_mpg_b already exists");
			}
			System.out.println(entityMap);
			response = atlasClient.createEntity(tag_rpm_b);
			System.out.println(response);
			if(response.size() > 0){
				String tag_rpm_b_guid = response.get(0);
				tag_rpm_b = atlasClient.getEntity(tag_rpm_b_guid);
				Id tag_rpm_b_id = tag_rpm_b.getId();
				entityMap.put("tag_rpm_b", tag_rpm_b);
				entityMap.put("tag_rpm_b_id", tag_rpm_b_id);
			}else{
				System.out.println("***************** tag_rpm_b already exists");
			}
			System.out.println(entityMap);
			//System.out.println("***************** " + InstanceSerialization.toJson(truck_a,true));
			response = atlasClient.createEntity(truck_a);
			System.out.println(response);
			if(response.size() > 0){
				String truck_a_guid = response.get(0);
				truck_a = atlasClient.getEntity(truck_a_guid);
				Id truck_a_id =truck_a.getId();
				entityMap.put("truck_a", truck_a);
				entityMap.put("truck_a_id", truck_a_id);
			}else{
				System.out.println("***************** truck_a already exists");
			}
			System.out.println(entityMap);
			//System.out.println("***************** " + InstanceSerialization.toJson(truck_b,true));
			response = atlasClient.createEntity(truck_b);
			System.out.println(response);
			if(response.size() > 0){
				String truck_b_guid = response.get(0);
				truck_b = atlasClient.getEntity(truck_b_guid);
				Id truck_b_id = truck_b.getId();
				entityMap.put("truck_b", truck_b);
				entityMap.put("truck_b_id", truck_b_id);
			}else{
				System.out.println("***************** truck_b already exists");
			}
			System.out.println(entityMap);
			response = atlasClient.createEntity(mine_a);
			System.out.println(response);
			if(response.size() > 0){
				String mine_a_guid = response.get(0);
				mine_a = atlasClient.getEntity(mine_a_guid);
				Id mine_a_id = mine_a.getId();
				entityMap.put("mine_a", mine_a);
				entityMap.put("mine_a_id", mine_a_id);
			}else{
				System.out.println("***************** mine_a already exists");
			}
			System.out.println(entityMap);
			response = atlasClient.createEntity(mine_b);
			System.out.println(response);
			if(response.size() > 0){
				String mine_b_guid = response.get(0);
				mine_b = atlasClient.getEntity(mine_b_guid);
				Id mine_b_id = mine_b.getId();
				entityMap.put("mine_b", mine_b);
				entityMap.put("mine_b_id", mine_b_id);
			}else{
				System.out.println("***************** mine_b already exists");
			}
			System.out.println(entityMap);
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		}
    	
		System.out.println("***************** Mine A: " + InstanceSerialization.toJson(mine_a, true));
    	System.out.println("***************** Mine B: " + InstanceSerialization.toJson(mine_b, true));
    	System.out.println("***************** Truck A: " + InstanceSerialization.toJson(truck_a, true));
    	System.out.println("***************** Truck B: " + InstanceSerialization.toJson(truck_b, true));
	}
	
	public void associateEntities() throws AtlasException {
		List<Id> trucks_mine_a = new ArrayList<Id>();
		List<Id> trucks_mine_b = new ArrayList<Id>();
		List<Id> mines_truck_a = new ArrayList<Id>();
		List<Id> mines_truck_b = new ArrayList<Id>();
		
		System.out.println(entityMap);
		
		try {
			Referenceable truck_a = (Referenceable)entityMap.get("truck_a");
			Referenceable truck_b = (Referenceable)entityMap.get("truck_b");
			Referenceable mine_a = (Referenceable)entityMap.get("mine_a");
			Referenceable mine_b = (Referenceable)entityMap.get("mine_b");
			
			if(entityMap.get("truck_a_id") != null && 
			   entityMap.get("truck_b_id") != null &&
			   entityMap.get("mine_a_id") != null && 
			   entityMap.get("mine_b_id") != null	 ){
				trucks_mine_a.add((Id)entityMap.get("truck_a_id"));
				trucks_mine_b.add((Id)entityMap.get("truck_b_id"));
				mines_truck_a.add((Id)entityMap.get("mine_a_id"));
				mines_truck_b.add((Id)entityMap.get("mine_b_id"));
				
				truck_a.set("parent_assets", mines_truck_a);
				truck_a.set("child_assets", null);
				truck_a.set("historian_tags", null);
				
				truck_b.set("parent_assets", mines_truck_b);
				truck_b.set("child_assets", null);
				truck_b.set("historian_tags", null);
				
				System.out.println("***************** " + InstanceSerialization.toJson(truck_a,true));
				System.out.println("***************** " + InstanceSerialization.toJson(truck_b,true));
				
				atlasClient.updateEntities(truck_a);
				atlasClient.updateEntities(truck_b);
			}
			
			if(entityMap.get("mine_a_id") != null && entityMap.get("mine_b_id") != null){
				mine_a.set("parent_assets", null);
				mine_a.set("child_assets", trucks_mine_a);
				mine_a.set("historian_tags", null);
				
				mine_b.set("parent_assets", null);
				mine_b.set("child_assets", trucks_mine_b);
				mine_b.set("historian_tags", null);
				
				System.out.println("***************** " + InstanceSerialization.toJson(mine_a,true));
				System.out.println("***************** " + InstanceSerialization.toJson(mine_b,true));
				
				atlasClient.updateEntities(mine_a);
				atlasClient.updateEntities(mine_b);
			}
			
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
	}
    
	public void deleteHistorianData(){
		try {
			atlasClient.deleteEntity(HistorianDataTypes.HISTORIAN_ASSET.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_asset.mine_a");
			atlasClient.deleteEntity(HistorianDataTypes.HISTORIAN_ASSET.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_asset.mine_b");
			atlasClient.deleteEntity(HistorianDataTypes.HISTORIAN_ASSET.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_asset.truck_a");
			atlasClient.deleteEntity(HistorianDataTypes.HISTORIAN_ASSET.getName(), AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "historian_asset.truck_b");
		} catch (AtlasServiceException e) {
			e.printStackTrace();
		}
	}
}
