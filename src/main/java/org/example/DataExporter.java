package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataExporter {
    public static void exportData(Dataset<Row> df) {
        df.write().option("header", "true").csv("output/cleaned_data.csv");
    }

    public static void exportAggregations(Dataset<Row> df, String filename) {
        df.write().option("header", "true").csv("output/" + filename);
    }
}