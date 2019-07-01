import java.sql.*;
import java.text.SimpleDateFormat;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

//Max Faust
//take data from vsc file, check it, put in db or
//out into fail vsc. count all the stuff too :)

public class Main {

	public static void main(String[] args) throws IOException {

		String csvFile = "ms3Interview.csv";
		String line = "";
		String cvsSplitBy = ",";
		String writeTo = "bad-data-";

		int goodData = 0;
		int badData = 0;
		int totalData = 0;
		int skip = 0;
		int bad = 0;

		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
		System.out.println(timeStamp);

		// create file name
		writeTo = writeTo.concat(timeStamp).concat(".csv");

		// create file to write bad-data to, open writer
		FileWriter csvWriter = new FileWriter(writeTo);

		// parse data
		try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

			while ((line = br.readLine()) != null) {

				String[] split = line.split(cvsSplitBy);

				if (skip == 0) {

					// write first line to bad-data file (index)
					for (int i = 0; i < split.length; i++) {

						if (i > 0) {

							csvWriter.write(",");
						}

						csvWriter.write(split[i]);
					}
					
					csvWriter.write("\n");
				}

				// skip index line
				if (skip > 0) {

					totalData++;

					// check data formatting
					for (int i = 0; i < 10; i++) {

						if (split[i].isEmpty()) {

							bad = 1;
							break;
						}
					}

					// write good data to db
					if (bad == 0) {

						goodData++;

						// write to database

						System.out.println("Line #" + skip);

						System.out.println(split[0] + " " + split[1] + " " + split[2] + " " + split[3] + " " + split[4]
								+ " " + split[5] + " " + split[6] + " " + split[7] + " " + split[8] + " " + split[9]);

						System.out.println("total: " + totalData + " bad: " + badData + " good: " + goodData);

						// write bad data to csv
					} else if (bad == 1) {

						badData++;
						bad = 0;

						for (int i = 0; i < split.length; i++) {

							if (i > 0) {

								csvWriter.write(",");
							}

							csvWriter.write(split[i]);
						}
						
						csvWriter.write("\n");
					}

				}

				skip++;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		// write to log

		csvWriter.flush();
		csvWriter.close();

	}

}
