import java.sql.*;
import java.text.SimpleDateFormat;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.commons.csv.*;

import java.io.FileReader;
import java.io.IOException;

import java.io.Reader;

//Max Faust

public class Main {

	public static void main(String[] args) throws IOException {

		String csvFile = "ms3Interview.csv";
		String writeTo = "bad-data-";
		String dbOut = " ";

		int goodData = 0;
		int badData = 0;
		int totalData = 0;
		int bad = 0;

		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());

		// create file name
		writeTo = writeTo.concat(timeStamp).concat(".csv");

		// create file to write bad-data to, open writer
		FileWriter csvWriter = new FileWriter(writeTo);

		connect();

		// read in csv file
		Reader in = new FileReader(csvFile);
		CSVParser parser = new CSVParser(in, CSVFormat.DEFAULT);
		List<CSVRecord> list = parser.getRecords();

		// check data quality, and write to file/db
		for (int i = 0; i < list.size(); i++) {
			
			dbOut = "INSERT INTO" + "VALUES ("; 

			System.out.println(list.get(i).get(0));

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
				} else if (bad == 0) {

					// write good data to db
					
//					if(j < 9) {
//						
//						dbOut.concat(list.get(i).get(j) + ",");
//					}else if(j == 9) {
//						
//						dbOut.concat(list.get(i).get(j));
//					}
					
					
					goodData++;
				}
			}

			// reset bad data flag
			if (bad == 1) {

				csvWriter.write("\n");
				bad = 0;
			}

			totalData++;
		}

		// write statistics to log fil

		parser.close();

		csvWriter.flush();
		csvWriter.close();

	}

	// connect to sqlite in memory db
	public static void connect() {

		Connection conn = null;
		try {

			conn = DriverManager.getConnection("jdbc:sqlite::memory:");
			System.out.println("Connection to SQLite has been established.");
		} catch (SQLException e) {

			System.out.println(e.getMessage());
		} finally {

			try {

				if (conn != null) {

					conn.close();
				}
			} catch (SQLException ex) {

				System.out.println(ex.getMessage());
			}
		}
	}

}
