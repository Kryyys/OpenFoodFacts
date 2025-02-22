package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class DataTransformer {
    public static Dataset<Row> transformData(Dataset<Row> df) {
        // Affichage de la rentrée dans le TRANSFORMER
        System.out.println("TRANSFORMER");
        df.printSchema();

        // Ajouter une colonne "is_healthy" (produit considéré sain si sucre < 5g et gras < 3g)
        df = df.withColumn("is_healthy",
                functions.when(df.col("sugars_100g").lt(5).and(df.col("fat_100g").lt(3)), "yes")
                        .otherwise("no"));

        // Ajouter une colonne "ingredient_count" (nombre d'ingrédients)
        df = df.withColumn("ingredient_count", functions.size(functions.split(df.col("ingredients_text"), ",")));

        // Filtrer uniquement les produits disponibles en France
        df = df.filter(df.col("countries").contains("france"));

        return df;
    }
}