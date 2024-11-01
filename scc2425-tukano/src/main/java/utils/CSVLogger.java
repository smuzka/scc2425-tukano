package utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class CSVLogger {

    private final String filePath = "logs.csv";

    // Constructor that sets the file path
    public CSVLogger() {}

    // Method to log a string and an integer to the CSV file
    public void logToCSV(String text, long number) {
        try (FileWriter fileWriter = new FileWriter(filePath, true);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
             PrintWriter printWriter = new PrintWriter(bufferedWriter)) {

            // Write to the CSV file in "text,number" format
            printWriter.printf("%s,%d%n", text, number);

        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
        }
    }
}