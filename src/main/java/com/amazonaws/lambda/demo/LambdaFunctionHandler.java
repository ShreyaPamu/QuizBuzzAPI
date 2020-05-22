package com.amazonaws.lambda.demo;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.uuid.Generators;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;


public class LambdaFunctionHandler  implements RequestHandler<Object, String>,Constants {
      static Context context;
      JSONObject error=new JSONObject("{\"error\":\"Invalied request\"}");
	  static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
	  static DynamoDB dynamoDB = new DynamoDB(client);
	  JSONObject json;
	    
    @Override
    public String handleRequest(Object input, Context cxt) {
     context=cxt;
     context.getLogger().log("Input"+input);
     try {
    	 json=new JSONObject(""+input);
    	 return getData(json).toString(); 
     }catch(JSONException e) {
    	 e.printStackTrace();
     } catch (Exception e) {
		e.printStackTrace();
	}
	return "Invalied Request";
   
    }

//This method calls appropriate API method 
	private Object getData(JSONObject input) throws Exception{
		if(input==null) {
			return error;
		}else {
			try {
				switch(input.getString(name)) {
				case "getUser":return getUser(input.getString(userName).toLowerCase());
				case "getQuiz": return getQuiz();
				case "setScore":return setScore(input.getString(userId),input.getNumber(score),input.getNumber(clientId));	
				case "getHistory":return getHistory(input.getString(userId));
				default:return error;
				}
			}catch(JSONException e) {
				e.printStackTrace();
			}	
		}
		return error;
	}
	
//This method returns userID of the given username
	private Object getUser(String username) throws Exception{
    	Table table = dynamoDB.getTable(userTable);
		Item item=getItemHelper(username, table);
	  if(item==null){
		      String uid=createuser(username);
	           if(uid!=null) {
	        	   try {
						Item i=table.getItem(userId,uid,userId,null);
						context.getLogger().log(i.toJSONPretty());
						return new JSONObject(""+i.toJSONPretty());
				   }catch (JSONException e) {
						context.getLogger().log("can't get the"+uid);
						e.printStackTrace();
				   }
	           }
	  }else {
		  try {
			  	String uid=(String) item.get(userId);
			  	Item i=table.getItem(userId,uid,userId,null);
				context.getLogger().log(i.toJSONPretty());
				return new JSONObject(""+i.toJSONPretty());
			}catch (JSONException e) {
				context.getLogger().log("can't get the player");
				e.printStackTrace();
			}
	  }
	return error; 
    }
	
//This is a private service called internally by getUser() to create a new use record and returns unique userID		
    private String createuser(String uName) throws Exception{
    	 Table table = dynamoDB.getTable(userTable);
	     try{ 
	    	 UUID uuid1 = Generators.timeBasedGenerator().generate();
	    	 String uId=uuid1.toString();
	    	 
	    	 Item item = new Item()
	    			    .withPrimaryKey(userId, uId)
	    			    .withString(userName,uName);
	    			              
	    	 table.putItem(item);
	    	 return uId;
	    	 }catch(Exception e) {
	    		 context.getLogger().log("Error in creating item");
			     e.printStackTrace();
		     }
    	 return null;
    }
     
	
//This method searches for a username and returns the corresponding Item
    private Item getItemHelper(String uname ,Table table) throws Exception{
	   	 Index index = table.getIndex("userName-index");
	   	 QuerySpec spec = new QuerySpec()
	   			    .withKeyConditionExpression("#nm = :username")
	   			    .withNameMap(new NameMap()
	   			    .with("#nm", userName))
	   			    .withValueMap(new ValueMap()
	   			    .withString(":username",uname));
	   	 ItemCollection<QueryOutcome> items = index.query(spec);
	   	 for(Item i:items) {
	   		 return i;
	   	 }
	   	 return null;
	 }
    
//This method sets the score, clientID ,date of the game played in the scores list of user record   
	private Object setScore(String uId, Number uscore, Number cId) throws Exception{
		Table table = dynamoDB.getTable(userTable);
		try {
			
			JSONObject alert=new JSONObject("{\"error\":\"Invalied userId\"}");
			Item itm = table.getItem("userId",uId);
			if(itm == null) return alert;
			
			Date today = new Date();
			DateFormat df = new SimpleDateFormat("MMMM dd yyyy , hh:mm a");
		    df.setTimeZone(TimeZone.getTimeZone("Asia/Kolkata"));
		    String IST = df.format(today);
			 
			JSONArray scoresArray = new JSONArray();
         	scoresArray.put(new JSONObject().put(clientId,cId)
         	                           .put(score,uscore)
         	                           .put(datetimeString,IST));

            UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey(userId, uId)
                                .withNameMap(new NameMap().with("#P", "scores"))
                                .withValueMap(new ValueMap()
                                .withJSON(":val", scoresArray.toString()) 
                                .withList(":empty_list", new ArrayList<>()))
                                .withUpdateExpression("SET #P = list_append(if_not_exists(#P, :empty_list), :val)");
            table.updateItem(updateItemSpec);
            return true;
		}catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

//This method gets the previous user game history	
	private Object getHistory(String uId) throws Exception{
		Table table = dynamoDB.getTable(userTable);
    	try {

    		Item item = table.getItem(userId,uId,"scores",null);
    		return new JSONObject("" + item.toJSONPretty());
    	}catch(Exception e) {
    		context.getLogger().log("GetItem Failed");
    		context.getLogger().log(e.getMessage());
    	}
    	return error;
	}

//This method randomly picks a set of questions based on the selected questionBank 
	private Object getQuiz() throws Exception{
		String qbName = "default";
		Table table1=dynamoDB.getTable(QuestionBankTable);
		Table table2=dynamoDB.getTable(QuestionsTable);
		Index index = table1.getIndex("questionBankName-index");
	   	 QuerySpec spec = new QuerySpec()
	   			    .withKeyConditionExpression("#nm = :questionBankName")
	   			    .withNameMap(new NameMap()
	   			    .with("#nm", questionBankName))
	   			    .withValueMap(new ValueMap()
	   			    .withString(":questionBankName",qbName));
	   	 ItemCollection<QueryOutcome> items = index.query(spec);
	   	 Item QBitem=new Item();
	   	 for(Item i:items) {
	   		 QBitem=i;
	   	 }
		 Number QBid = QBitem.getNumber(questionBankID);
	     Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
	     expressionAttributeValues.put(":qbid", QBid);

	     ItemCollection<ScanOutcome> itm = table2.scan("questionBankID = :qbid",questionID,null,expressionAttributeValues);
	     ArrayList<Number> al = new ArrayList<Number>();
         for(Item i : itm) al.add(i.getNumber(questionID));
         JSONArray obj=getRandomQuiz(al);
		return obj;	
	}	

//This is a private service called by getQuiz
	private JSONArray getRandomQuiz(ArrayList<Number> al) throws Exception{
		Random r = new Random();
		HashSet<Integer> hs = new HashSet<Integer>();
        context.getLogger().log(al.toString());
	    while(hs.size()!=5) {
	    	hs.add(r.nextInt(al.size()));	
	    }
	    Table table=dynamoDB.getTable(QuestionsTable);
		JSONArray ja = new JSONArray();
		for(int idx:hs) {
			Number qid=al.get(idx);
			Item item = table.getItem(questionID,qid);
			ja.put(new JSONObject(""+item.toJSONPretty()));
		}
		return ja;
	}
  
}

//"{\"name\":\"getUser\",\"userName\":\"c\"}"
//"{\"name\":\"getHistory\",\"userId\":\"415d901e-9be8-11ea-9602-fd6fd939acb4\"}"


//"{\"name\":\"getQuiz\",\"questionBankName\":\"default\"}"
//"{\"name\":\"setScore\",\"userId\":\"415d901e-9be8-11ea-9602-fd6fd939acb4\",\"score\":\"5\",\"clientId\":\"2\"}"

//  "{\"name\":\"${input.params('name')}\",\"playerid\":\"${input.params('pid')\"}}"