package poke.server.storage;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;

public class MongoStorage {
	private static MongoClient mongoClient;
	private static DB db;
	private static DBCollection collUsers = null;
	private static DBCollection collCoureses = null;
	private static DBCollection collFiles = null;
	private static DBCollection collQuestions = null;
	private static DBCollection collAnswers = null;
	public MongoStorage(String ip){
		try {
			mongoClient = new MongoClient( ip , 27017 );
			db = mongoClient.getDB( "test" );
	    	collUsers = db.getCollection("users");
	    	collCoureses = db.getCollection("courses");
	    	collFiles = db.getCollection("files");
	    	collQuestions = db.getCollection("questions");
	    	collAnswers = db.getCollection("answers");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}/*
		collUsers.drop();
		collCoureses.drop();
		collFiles.drop();
		collQuestions.drop();
		collAnswers.drop();
		BasicDBObject counter = new BasicDBObject("counter",0);
		collUsers.insert(counter);
		collCoureses.insert(counter);
		collFiles.insert(counter);
		collQuestions.insert(counter);
		collAnswers.insert(counter);
		*/
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MongoStorage DB = new MongoStorage("localhost");
		
		collUsers.drop();
		collCoureses.drop();
		collFiles.drop();
		collQuestions.drop();
		collAnswers.drop();
		BasicDBObject counter = new BasicDBObject("counter",0);
		collUsers.insert(counter);
		collCoureses.insert(counter);
		collFiles.insert(counter);
		collQuestions.insert(counter);
		collAnswers.insert(counter);
		
		addUser("test@test.com", "secret", "first", "test");
		addUser("test2@test.com", "secret", "second", "test");
		printAll(collUsers);
		updateUser("test2@test.com", "password", "boo");
		BasicDBObject user = getUser("test2@test.com");
//		System.out.println(user.getString("password"));
		
		addCourse("CMPE275", "275","Distributed component design, scalability, messaging, and integration practices.", new ArrayList<Integer>(), "22nd Jan", "05/20/2014");
		addCourse("CMPE203", "203", "Development of software systems from the perspective of project management.",new ArrayList<Integer>(), "22nd Jan", "05/20/2014");
		addMemberToCourse(1,1);
		addMemberToCourse(1,2);
		addMemberToCourse(2,1);
		printAll(collCoureses);
		List<DBObject> userCourses = getCoursesByuId(1);
		for (int i=0; i< userCourses.size();i++){
//			System.out.println((Integer)userCourses.get(i).get("cId")+". "+(String)userCourses.get(i).get("name")+(String)userCourses.get(i).get("code"));
		}
		removeMemberFromCourse(1, 1);
//		printAll(collCoureses);
		
		addFile("test.txt", 1, "3/27/2014");
		addFile("test2.txt", 2, "3/27/2014");
		addFile("test3.txt", 2, "3/27/2014");
		printAll(collFiles);
		List<DBObject> userFiles = getFileByuId(2);
		for (int i=0; i< userFiles.size();i++){
//			System.out.println((Integer)userFiles.get(i).get("fId")+". "+(String)userFiles.get(i).get("name"));
		}
	
		addQuestion("add&drop", 1,  "when is the last day for add/drop", "3/27/2014");
		addQuestion("Add Code", 2,  "How can I get an add code?", "3/27/2014");
		addAnswer(2, 1, "Last date is April 4th");
		addAnswer( 1, 2, "Fill in the add code form.");
		addAnswer(2, 1, "Beg the professor");
//		deleteAnswer(3);
		List<DBObject> answers =getAnswersByqId(1);
		for (int i=0; i< answers.size();i++){
//			System.out.println((Integer)answers.get(i).get("aId")+". "+(String)answers.get(i).get("description"));
		}
		
//		deleteQuestion(1);
		printAll(collQuestions);
		printAll(collAnswers);

		if (validateUser("test@test.com", "secret")) 
			System.out.println("User successfully validated");
	}

	public static void printAll(DBCollection coll){
		DBCursor cursor = coll.find();
		try {
		   while(cursor.hasNext()) {
		       System.out.println(cursor.next());
		   }
		} finally {
		   cursor.close();
		}
	}
	public static void addUser(String email, String password, String firstName, String lastName){
		BasicDBObject user = new BasicDBObject();
		user.append("uId", counterInc("users"));
		user.append("email", email);
		user.append("password", password);
		user.append("firstName", firstName);
		user.append("lastName", lastName);
		collUsers.insert(user);
	}
	public static BasicDBObject getUser(String email){
		BasicDBObject query = new BasicDBObject("email", email);
		return (BasicDBObject) collUsers.findOne(query);
	}
	public static Integer getUserID(String email){
		BasicDBObject query = new BasicDBObject("email", email);
		return ((BasicDBObject)collUsers.findOne(query)).getInt("uId");
	}
	public static void deleteUser(String email){
		BasicDBObject user = new BasicDBObject("email", email);
		collUsers.remove(user);
	}
	public static void updateUser(String email, String attr, String value){
		BasicDBObject query = new BasicDBObject("email", email);
		BasicDBObject change = new BasicDBObject(attr, value);
		BasicDBObject newDoc = new BasicDBObject().append("$set", change);
    	collUsers.update(query,newDoc);
	}
	public static boolean validateUser(String email,String password){
		BasicDBObject query = new BasicDBObject("email", email);
		BasicDBObject result = (BasicDBObject) collUsers.findOne(query);
		if (result==null) return false;
		return result.getString("password").equals(password);
	}
	public static void addCourse(String name, String code, String desc,ArrayList<Integer> members, String startDate, String endDate){
		BasicDBObject course = new BasicDBObject();
		course.append("cId", counterInc("courses"));
		course.append("name", name);
		course.append("code", code);
		course.append("desc", desc);
		course.append("members", members);
		course.append("startDate", startDate);
		course.append("endDate", endDate);
		collCoureses.insert(course);
	}
	public static BasicDBObject getCourseBycId(int cId){
		BasicDBObject query = new BasicDBObject("cId", cId);
		return (BasicDBObject) collCoureses.findOne(query);
	}
	public static BasicDBObject getCourseByName(String name){
		BasicDBObject query = new BasicDBObject("name", name);
		return (BasicDBObject) collCoureses.findOne(query);
	}
	public static List<DBObject> getCoursesByuId(int uId){
		BasicDBObject query = new BasicDBObject("members", uId);
		return collCoureses.find(query).toArray();
	}
	public static List<DBObject> getAllCourses(){
		return collCoureses.find().toArray();
	}
	public static void deleteCourse(int cId){
		BasicDBObject course = new BasicDBObject("cId", cId);
		collCoureses.remove(course);
	}
	public static void updateCourse(int cId, String attr, String value){
		BasicDBObject query = new BasicDBObject("cId", cId);
		BasicDBObject change = new BasicDBObject(attr, value);
		BasicDBObject newDoc = new BasicDBObject().append("$set", change);
    	collCoureses.update(query,newDoc);
	}
	public static void addMemberToCourse(int cId, int uId){
		BasicDBObject query = new BasicDBObject("cId", cId);
		BasicDBObject change = new BasicDBObject("members", uId);
		BasicDBObject newDoc = new BasicDBObject().append("$push", change);
    	collCoureses.update(query,newDoc);
	}
	public static void removeMemberFromCourse(int cId, int uId){
		ArrayList<Integer> temp;
		temp = (ArrayList<Integer>)getCourseBycId(cId).get("members");
		temp.remove(uId);
		BasicDBObject query = new BasicDBObject("cId",cId);
		BasicDBObject change = new BasicDBObject("members",temp);
		BasicDBObject newDoc = new BasicDBObject().append("$set", change);
		collCoureses.update(query,newDoc);
	}
	public static ArrayList<Integer> getMembersForCourse(int cId){
		ArrayList<Integer> temp;
		temp = (ArrayList<Integer>)getCourseBycId(cId).get("members");
		return temp;
	}
	public static void addFile(String name, int owner, String uploadDate){
		BasicDBObject file = new BasicDBObject();
		file.append("fId", counterInc("files"));
		file.append("name", name);
		file.append("owner", owner);
		file.append("uploadDate", uploadDate);
		collFiles.insert(file);
	}
	public static BasicDBObject getFileByfId(int fId){
		BasicDBObject query = new BasicDBObject("fId", fId);
		return (BasicDBObject) collFiles.findOne(query);
	}
	public static List<DBObject> getFileByuId(int uId){
		BasicDBObject query = new BasicDBObject("owner", uId);
		return collFiles.find(query).toArray();
	}
	public static void deleteFile(int fId){
		BasicDBObject file = new BasicDBObject("fId", fId);
		collFiles.remove(file);
	}
	public static void updateFile(int fId, String attr, String value){
		BasicDBObject query = new BasicDBObject("fId", fId);
		BasicDBObject change = new BasicDBObject(attr, value);
		BasicDBObject newDoc = new BasicDBObject().append("$set", change);
    	collFiles.update(query,newDoc);
	}
	public static void addQuestion(String title, int owner, String description, String postDate){
		BasicDBObject question = new BasicDBObject();
		question.append("qId", counterInc("questions"));
		question.append("title", title);
		question.append("owner", owner);
		question.append("description", description);
		question.append("postDate", postDate);
		collQuestions.insert(question);
	}
	public static BasicDBObject getQuestionByqId(int qId){
		BasicDBObject query = new BasicDBObject("qId", qId);
		return (BasicDBObject) collQuestions.findOne(query);
	}
	public static List<DBObject> getQuestionsByuId(int uId){
		BasicDBObject query = new BasicDBObject("owner", uId);
		return collQuestions.find(query).toArray();
	}
	public static List<DBObject> getAllQuestions(){
		return collQuestions.find().toArray();
	}
	//Fix this
	public static void deleteQuestion(int qId){
		BasicDBObject question = new BasicDBObject("qId", qId);
		List<DBObject> answers = getAnswersByqId(qId);
		for (int i = 0; i<answers.size();i++){
			deleteAnswer(((BasicDBObject)answers.get(i)).getInt("aId"));
		}
		collQuestions.remove(question);
	}
	public static void updateQuestion(int qId, String attr, String value){
		BasicDBObject query = new BasicDBObject("qId", qId);
		BasicDBObject change = new BasicDBObject(attr, value);
		BasicDBObject newDoc = new BasicDBObject().append("$set", change);
    	collQuestions.update(query,newDoc);
	}/*
	public static void addAnswerToQuestion(int qId, int aId){
		BasicDBObject query = new BasicDBObject("qId", qId);
		BasicDBObject change = new BasicDBObject("answers", aId);
		BasicDBObject newDoc = new BasicDBObject().append("$push", change);
    	collQuestions.update(query,newDoc);
	}
	public static void removeAnswerFromQuestion(int qId, int aId){
		ArrayList<Integer> temp;
		temp = (ArrayList<Integer>)getQuestionByqId(qId).get("answers");
		temp.remove(aId);
		BasicDBObject query = new BasicDBObject("qId",qId);
		BasicDBObject change = new BasicDBObject("answers",temp);
		BasicDBObject newDoc = new BasicDBObject().append("$set", change);
		collQuestions.update(query,newDoc);
	}
	public static ArrayList<Integer> getAnswersForQuestion(int qId){
		ArrayList<Integer> temp;
		temp = (ArrayList<Integer>)getQuestionByqId(qId).get("answers");
		return temp;
	}*/
	public static void addAnswer(int uId, int qId, String description){
		BasicDBObject answer = new BasicDBObject();
		answer.append("aId", counterInc("answers"));
		answer.append("uId", uId);
		answer.append("qId", qId);
		answer.append("description", description);
		collAnswers.insert(answer);
	}
	public static BasicDBObject getAnswer(int aId){
		BasicDBObject query = new BasicDBObject("aId", aId);
		return (BasicDBObject) collAnswers.findOne(query);
	}
	public static List<DBObject> getAllAnswer(){
		return collAnswers.find().toArray();
	}
	public static List<DBObject> getAnswersByqId(int qId){
		BasicDBObject query = new BasicDBObject("qId", qId);
		return collAnswers.find(query).toArray();
	}
	public static void deleteAnswer(int aId){
		BasicDBObject answer = new BasicDBObject("aId", aId);
		collAnswers.remove(answer);
	}
	public static void updateAnswer(int aId, String attr, String value){
		BasicDBObject query = new BasicDBObject("aId", aId);
		BasicDBObject change = new BasicDBObject(attr, value);
		BasicDBObject newDoc = new BasicDBObject().append("$set", change);
    	collAnswers.update(query,newDoc);
	}
	public static Integer counterInc(String collName){
		DBCollection coll = db.getCollection(collName);
		BasicDBObject query = new BasicDBObject();
    	BasicDBObject field = new BasicDBObject();
    	field.put("counter", 1);
    	DBCursor cursor = coll.find(query,field);
    	int count=99;
    	BasicDBObject obj = (BasicDBObject) cursor.next();
    	count=obj.getInt("counter");
    	BasicDBObject countQuery = new BasicDBObject("counter", count);
    	BasicDBObject newDoc = new BasicDBObject("$set", new BasicDBObject("counter",++count));
    	coll.update(countQuery,newDoc );
    	return count;
	}
}
