package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataExporter {
    public static void exportData(Dataset<Row> df) {
        // Affichage de la rentrée dans le EXPORTER
        System.out.println("EXPORTER");
        df.printSchema();

        df.coalesce(1).write().mode("overwrite").option("header", "true").csv("output/cleaned_data.csv");
    }

    public static void exportAggregations(Dataset<Row> df, String filename) {
        df.coalesce(1).write().mode("overwrite").option("header", "true").csv("output/" + filename);
    }
}