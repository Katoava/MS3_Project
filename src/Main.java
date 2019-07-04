import java.sql.*;
import java.text.SimpleDateFormat;
import java.io.*;

import java.util.List;

import org.apache.commons.csv.*;

import java.io.FileReader;
import java.io.IOException;

import java.io.Reader;

//Max Faust

public class Main {

	private static Connection conn;

	public static void main(String[] args) throws IOException, SQLException {
		
		if(args.length == 0) {
			System.out.println("Please specify a CSV file.");
			System.exit(0);
		}
		
		String csvFile = args[0];
		String writeTo = "bad-data-";

		int goodData = 0;
		int badData = 0;
		int totalData = 0;
		int bad = 0;

		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());

		// create file name
		writeTo = writeTo.concat(timeStamp).concat(".csv");

		// create file to write bad-data to, open writer
		FileWriter csvWriter = new FileWriter(writeTo);
		
		FileWriter fw=new FileWriter("LOG");    

		connect();

		// read in csv file
		Reader in = new FileReader(csvFile);
		CSVParser parser = new CSVParser(in, CSVFormat.DEFAULT);
		List<CSVRecord> list = parser.getRecords();

		// check data quality, and write to file/db
		for (int i = 0; i < list.size(); i++) {

			for (int j = 0; j < 10; j++) {

				// write index to bad-data
				if (i == 0) {

					if (j > 0) {

						csvWriter.write(",");
					}

					csvWriter.write(list.get(i).get(j));

					if (j == 9) {
						csvWriter.write("\n");
					}
				}

				// write out bad data
				if (bad == 1) {

					if (j > 0) {

						csvWriter.write(",");
					}

					if (list.get(i).get(j).contains(",")) {

						csvWriter.write("\"");
						csvWriter.write(list.get(i).get(j));
						csvWriter.write("\"");
					} else {

						csvWriter.write(list.get(i).get(j));
					}

				}

				// signal bad data
				if (list.get(i).get(j).isEmpty() && bad != 1) {

					bad = 1;
					j = -1;
					badData++;
				}
			}

			// write good data out
			if (bad != 1 && i != 0) {

				insert(list.get(i).get(0), list.get(i).get(1), list.get(i).get(2), list.get(i).get(3),
						list.get(i).get(4), list.get(i).get(5), list.get(i).get(6), list.get(i).get(7),
						list.get(i).get(8), list.get(i).get(9));

				goodData++;
			}

			// reset bad data flag
			if (bad == 1) {

				csvWriter.write("\n");
				bad = 0;
			}

			totalData++;
		}
		
		System.out.println("Writing to LOG");
		
		fw.write(timeStamp + ": \n");
		fw.write(totalData + " lines read.\n" + goodData + " lines added to database.\n" + badData + " lines written to " + writeTo + "\n");    
		
		System.out.println("See LOG for data statistics and " + writeTo + " for bad data entries.");
		
		parser.close();

		conn.close();

		csvWriter.flush();
		csvWriter.close();
		
		fw.flush();
		fw.close();

	}

	// connect to sqlite in memory db
	public static void connect() {

		Statement stmt = null;

		conn = null;
		try {

			conn = DriverManager.getConnection("jdbc:sqlite::memory:");

			if (conn != null) {

				System.out.println("Connection to SQLite has been established.");
			}

			stmt = conn.createStatement();

			String sql = "CREATE TABLE Data " + "(a VARCHAR(255), " + "b VARCHAR(255), " + "c VARCHAR(255), "
					+ "d VARCHAR(255), " + "e VARCHAR(2000), " + "f VARCHAR(255), " + "g VARCHAR(255), " + "h Int, "
					+ "i Int, " + "j VARCHAR(255))";

			stmt.executeUpdate(sql);

			if (stmt != null) {

				System.out.println("Created table Data in database.");
			}

		} catch (SQLException e) {

			System.out.println(e.getMessage());
		}
	}
	
	//Insert data into sql database. Convert bools to ints for storage. 
	public static int insert(String a, String b, String c, String d, String e, String f, String g, String h, String i,
			String j) {

		int numRowsInserted = 0;
		PreparedStatement ps = null;

		int hBool = 0;
		int iBool = 0;

		if (Boolean.parseBoolean(h)) {
			hBool = 1;
		} else {
			hBool = 0;
		}

		if (Boolean.parseBoolean(i)) {
			iBool = 1;
		} else {
			iBool = 0;
		}

		// alter this to do the thing I want, and alter the commands and stuff
		String INSERT_SQL = "INSERT INTO data(" + "a, b, c, d, e, f, g, h, i, j) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		try {

			ps = conn.prepareStatement(INSERT_SQL);

			ps.setString(1, a);
			ps.setString(2, b);
			ps.setString(3, c);
			ps.setString(4, d);
			ps.setString(5, e);
			ps.setString(6, f);
			ps.setString(7, g);
			ps.setInt(8, hBool);
			ps.setInt(9, iBool);
			ps.setString(10, j);

			numRowsInserted = ps.executeUpdate();
		} catch (SQLException ex) {

			ex.printStackTrace();
		} finally {

			try {

				if (ps != null) {

					ps.close();
				}
			} catch (SQLException ex) {

				ex.printStackTrace();
			}
		}
		
		return numRowsInserted;
	}
	
	
	//Query the db, for testing
	public static void test() {

		Statement stmt = null;
		ResultSet rs = null;
		
		try {
			
			stmt = conn.createStatement();
			rs = stmt.executeQuery("SELECT a, b, c, d, e, f, g, h, i, j FROM Data");
			
			while (rs.next()) {
		
				  String line = rs.getString("a");
				  System.out.println(line + "\n");
				}
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
	}

}
