package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class DataAggregator {
    public static Dataset<Row> getTopBrands(Dataset<Row> df) {
        return df.groupBy("brands").count()
                .orderBy(functions.desc("count"))
                .limit(10);
    }

    public static Dataset<Row> getAverageSugarEnergyByCountry(Dataset<Row> df) {
        return df.groupBy("countries")
                .agg(
                        functions.avg("sugars_100g").alias("avg_sugar"),
                        functions.avg("energy_100g").alias("avg_energy")
                );
    }

    public static Dataset<Row> getProductDistributionByLabels(Dataset<Row> df) {
        return df.groupBy("labels").count()
                .orderBy(functions.desc("count"));
    }
}