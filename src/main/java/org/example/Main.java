package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import java.util.Arrays;

public class Main {
    private static Dataset<Row> df;
    public static void main(String[] args) {
        String filePath = "resources/en.openfoodfacts.org.products.csv";
        System.setProperty("hadoop.home.dir", "C:\\Users\\marii\\AppData\\Local\\Programs\\hadoop-3.0.0");
        System.setProperty("hadoop.native.lib", "false");

        /* Initialise la session Spark */
        SparkSession spark = SparkSession.builder()
                .appName("OpenFoodFact")
                .master("local") // Exécute Spark en mode local
                .getOrCreate();

        // Chargement du fichier CSV
        df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", "\t")
                .load(filePath); // Utilisation de la variable

        // Affichage des colonnes brutes (pour essayer de résoudre le pb des colonnes)
        System.out.println("Colonnes brutes du DataFrame : " + Arrays.toString(df.columns()));
        df.printSchema();
        df.show();

        // Nettoyage
        df = DataCleaner.cleanData(df);

        // Transformation
        df = DataTransformer.transformData(df);

        // Export des données nettoyées
        DataExporter.exportData(df);

        // Agrégation et export des résultats
        Dataset<Row> topBrands = DataAggregator.getTopBrands(df);
        DataExporter.exportAggregations(topBrands, "top_brands.csv");

        Dataset<Row> avgSugarEnergy = DataAggregator.getAverageSugarEnergyByCountry(df);
        DataExporter.exportAggregations(avgSugarEnergy, "avg_sugar_energy_by_country.csv");

        Dataset<Row> productLabels = DataAggregator.getProductDistributionByLabels(df);
        DataExporter.exportAggregations(productLabels, "product_distribution_by_labels.csv");

        // Fermeture de Spark
        spark.stop();
    }
}
