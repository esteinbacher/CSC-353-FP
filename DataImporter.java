import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.sql.Types;
import java.util.Set;

import javax.print.DocFlavor.STRING;

import java.util.HashSet;

/**
 * Imports twitter data in CSV format into a database using JDBC.
 *
 * @author Hammurabi Mendes <hamendes@davidson.edu>
 */
public class DataImporter {
	private Connection connection;

	public HashSet<String> postingUserID = new HashSet<String>();
	public HashSet<String> tweetID = new HashSet<String>();
	public HashSet<String> statement = new HashSet<String>();
	/**
	 * Default constructor.
	 */
	public DataImporter() {
	}

	/**
	 * Creates a connection with the database, and calls the loadFile method,
	 * which actually performs the insertions.
	 */
	private void importData() {
		try {
			// Set password to null if there's no password for your database
			// We are passing some options to the driver manager:
			//  useUnicode = yes: because many tweets are written in languages other than English
			//                    and contain unicode characters
			//  characterEncoding = UTF8: encoding for foreign characters
			connection = DriverManager.getConnection("jdbc:mysql://localhost/TwitterDB?useUnicode=yes&characterEncoding=UTF-8", "root", "es4025es");

			connection.setAutoCommit(false);

			loadFile("NFL Play by Play 2009-2018.csv");
			
			connection.commit();

		}
		catch(Exception exception) {

			System.err.println("Caught exception below, stopping program.");
			System.out.println("SQLException: " + exception.getMessage());

			// Roll back if ANY error occurred above
			try {
				connection.rollback();
			}
			catch(SQLException innerException) {
				// SQLExceptionInception? Ignore.
			}

			if(exception instanceof SQLException) {
				System.out.println("SQLState: " + ((SQLException) exception).getSQLState());
				System.out.println("VendorError: " + ((SQLException) exception).getErrorCode());
			}

			exception.printStackTrace();
		}
	}

	private void loadFile(String filename) throws IOException, NumberFormatException, SQLException {
		System.out.println(filename);
		BufferedReader reader = new BufferedReader(new FileReader(filename));

		String line = null;
		String[] fields = new String[16];

		String command1 = "INSERT INTO  VALUES (?,?,?,?,?,?)";
		String command2 = "INSERT INTO tweet VALUES (?,?,?,?,?,?,?,?,?)";
		String command3 = "INSERT INTO hashtag VALUES (?,?,?,?)";
		String command4 = "INSERT INTO URL VALUES (?,?,?,?)";

		// Prepare the statements only ONE TIME
		PreparedStatement statement1 = connection.prepareStatement(command1);
		PreparedStatement statement2 = connection.prepareStatement(command2);
		PreparedStatement statement3 = connection.prepareStatement(command3);
		PreparedStatement statement4 = connection.prepareStatement(command4);
		
		while((line = reader.readLine()) != null ) {

			String[] temp = line.split(",");

			for(int i = 0; i < fields.length; i++) {
				if(i<temp.length) {
					if(temp[i].length() <2) {
						fields[i]= null;
					}
					else {
						fields[i]= temp[i];
					}
				}

			}

			//for(int i = 0; i < fields.length; i++) System.out.println(fields[i] + ""+ i);
			// TODO: your code here
			/*
			 * You can avoid that errors cascade into the main program by catching exceptions
			 * in the executeUpdate() calls. Make sure that the generated exceptions are only
			 * for appropriate reasons (i.e. tweets containing strings like "\xF0\x9F\x99\x8C\xF0\x9F", etc,
			 * not because there is something wrong with your data parsing).
			 */


			//Set User if doesn't already exist
			if(postingUserID.add(fields[0])){
				statement1.setString(1, fields[0]);
				statement1.setString(2, fields[1]);
				statement1.setString(3, fields[2]);
				statement1.setString(4, fields[3]);
				statement1.setString(5, fields[4]);
				statement1.setString(6, fields[5]);

				try {
					statement1.executeUpdate();
				}
				catch(Exception exception) {
					
					System.err.println(exception.getMessage());
				}
			}



			//Set tweet statement
			//taking into account lat and long
			if(tweetID.add(fields[6])) {
				statement2.setString(1, fields[0]);
				statement2.setString(2, fields[6]);
				statement2.setString(3, fields[7]);
				statement2.setString(4, fields[8]);
				statement2.setString(5, fields[9]);
				if(fields[10]==null) statement2.setString(6, "0");
				else statement2.setString(6, fields[10]);
				if(fields[11]==null) statement2.setString(7, "0");
				else statement2.setString(7, fields[11]);
				//latitude
				if(fields[12]==null){
					statement2.setNull(8, java.sql.Types.DECIMAL);
				}
				else{
					statement2.setString(8, fields[12]);
				}
				//longitude
				if(fields[13]== null){
					statement2.setNull(9, java.sql.Types.DECIMAL);
				}
				else{
					statement2.setString(9, fields[13]);
				}
				//execute
				try {
					statement2.executeUpdate();
				}
				catch(Exception exception) {
					System.out.println("here");
					System.err.println(exception.getMessage());
				}





				//hashtags
				if(fields[14] != null || fields[14] == ""){
					String[] hashtags = fields[14].split(" ");
					for(int i = 1; i<hashtags.length;i++){
						String[] index = hashtags[i].split("\\|");

						statement3.setString(1, fields[6]);
						statement3.setInt(2, i);
						statement3.setString(3, index[0]);
						statement3.setString(4, index[1]);
						statement.add(statement3.toString());
						try {
							statement3.executeUpdate();
						}
						catch(Exception exception) {

							System.err.println(exception.getMessage());
						}
					}
				}



				//URLs
				if(fields[15] != null||fields[15]==""){
					String[] URLs = fields[15].split(" ");

					for(int i = 1; i<URLs.length;i++){
						String[] index = URLs[i].split("\\|");
						statement4.setString(1, fields[6]);
						statement4.setInt(2, i);
						statement4.setString(3, index[0]);
						statement4.setString(4, index[1]);


						try {
							statement4.executeUpdate();
						}
						catch(Exception exception) {

							System.err.println(exception.getMessage());
						}
					}
				}


			}
			
		}

		reader.close();
	}

	public static void main(String[] arguments) {
		DataImporter importer = new DataImporter();

		importer.importData();
	}
}
