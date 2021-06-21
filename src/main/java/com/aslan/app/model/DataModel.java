package com.aslan.app.model;

import org.json.simple.JSONObject;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

//@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonInclude(value = JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataModel {
	private String name;
	private String message;
	//private JSONObject dt_model = new JSONObject();

	public String getName() { return name; }
	public void setName(String name) { this.name = name; }
	
	public String getMessage() { return message; }
	public void setMessage(String message) { this.message = message; }
	
	//public JSONObject getDt_model() { return dt_model; }
	//public void setDt_model(JSONObject dt_model) { this.dt_model = dt_model; }
	

} //end class
