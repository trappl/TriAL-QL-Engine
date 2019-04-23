package executor;

import data.structures.Tripel;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CreateData {
    public static JavaSparkContext ctx;
    public static SQLContext sqlContext;
    public static SparkSession sparkSession;

    public static void main(String args[]) throws Exception {

        String sourcename, targetname;

        if (args.length < 2) {
            System.err.println("to few arguments");
            System.exit(0);
        }
        sourcename = args[0];
        targetname = args[1];

        sparkSession = SparkSession.builder()
                .appName("JavaSparkSQL")
                .getOrCreate();

        ctx = new JavaSparkContext(sparkSession.sparkContext());
        sqlContext = new SQLContext(ctx);

        Dataset ds = sqlContext.createDataFrame(ctx.textFile(sourcename).coalesce(ctx.defaultParallelism()).map(new Function<String, String[]>() {
            public String[] call(String line) {
                return line.substring(0, line.length() -2).split("\t");
            }
        }).map(new Function<String[], Tripel>() {
            public Tripel call(String[] line) {
                return new Tripel(line[0], line[1], line[2]);
            }
        }), Tripel.class);
        ds.write().mode(SaveMode.Overwrite).partitionBy("predicate").parquet(targetname);
    }
}
