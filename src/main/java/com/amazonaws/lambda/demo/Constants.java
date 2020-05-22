package com.amazonaws.lambda.demo;

import org.json.JSONObject;


public interface Constants {
  
	public static JSONObject error=new JSONObject("{\"error\":\"Invalied request\"}");
	public static String userTable = "user";
	public static String QuestionsTable = "Questions";
	public static String QuestionBankTable = "QuestionBank";
	public static String name="name";
	public static String userName="userName";
	public static String userId="userId";
	public static String score="score";
	public static String clientId="clientId";
	public static String datetimeString="datetimeString";
	public static String questionBankID = "questionBankID";
	public static String questionBankName = "questionBankName";
	public static String questionID = "questionID";

}
