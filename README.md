# MS3_Project

Max Faust

This is a README for my MS3 interview project.

To run:
  Executable jar: 
    java -jar dataRip.jar "file path"
    
My thought process:
First, I set up the code to read the csv and look for good data and bad data. At first, I used a buffered reader to parse the file, but ran into a problem when dealing with double quoted lines, so I looked for another solution. This solution was OpenCSV, which made the process of reading the file extremely simple. From here, I reworked my method of sorting good data and bad data to fit the new parseing system that I had employed via OpenCSV. I wrote the bad data out to a file via csv writer and began to implement the sql side of the project. I decided to use JDBC, and after reading a few tutorials was able to connect to an in memory database easily. Then, I wrote a
method to insert data and altered my connect method to create a table. I then utilized this method to insert good data into the database.
I wrote a method to test my database by querying elements from it. 

Note: The first line of indexes will be written to the bad-data file. 
