package org.example;

import org.apache.spark.sql.*;
import java.text.Normalizer;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        // Initialisation de Spark
        SparkSession spark = SparkSession.builder()
                .appName("OpenFoodFactsIntegration")
                .config("spark.master", "local")
                .getOrCreate();

        // Chargement du fichier CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("resources/en.openfoodfacts.org.products.csv");

        // Affichage des colonnes brutes
        System.out.println("Colonnes brutes du DataFrame : " + Arrays.toString(df.columns()));
        df.printSchema();

        // Nettoyage des noms de colonnes (suppression des espaces et caractères spéciaux)
        String[] cleanedColumnNames = Arrays.stream(df.columns())
                .map(c -> Normalizer.normalize(c.trim(), Normalizer.Form.NFKC)) // Normalisation Unicode
                .map(c -> c.replaceAll("[^a-zA-Z0-9_]", "")) // Suppression des caractères spéciaux
                .toArray(String[]::new);
        df = df.toDF(cleanedColumnNames);

        // Affichage des colonnes après nettoyage
        System.out.println("Colonnes après nettoyage : ");
        for (String col : df.columns()) {
            System.out.println("'" + col + "'");
        }

        // Vérification du vrai nom de "product_name"
        for (String col : df.columns()) {
            if (col.toLowerCase().contains("product")) {
                System.out.println("🔍 Possible nom de 'product_name' trouvé : '" + col + "'");
            }
        }

        // Vérification de la présence de "product_name"
        if (!Arrays.asList(df.columns()).contains("product_name")) {
            System.err.println("⚠️ Erreur : 'product_name' n'est pas trouvée dans le DataFrame !");
            spark.stop();
            return;
        }

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
