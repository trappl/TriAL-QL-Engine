/*
* Copyright (C) 2017 University of Freiburg.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* The TriAL-QL Engine is a research project at the department of 
* computer science, University of Freiburg. 
*
* More information on the project:
* http://dbis.informatik.uni-freiburg.de/forschung/projekte/DiPoS/
* zablocki@informatik.uni-freiburg.de
*/

package executor;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import data.structures.Configuration;
import data.structures.QueryStruct;
import data.structures.ResultStruct;
import generator.ComposeQueries;
import parser.TriALQLClassListener;
import parser.TriALQLParseQuery;

/**
 * SPARK main execution class.
 */
public class AppSpark {

	public static SparkConf sparkConf;
	public static JavaSparkContext ctx;
	public static SQLContext sqlContext;
	public static boolean runOnSPARK;
	static boolean notExecuted = true;
	public static ArrayList<String> takePredicates = new ArrayList<String>();
	static String where = "";

	public static void main(String args[]) throws Exception {
		long t0 = System.nanoTime();

		if (args.length < 3) {
			System.err.println("to few arguments");
			System.exit(0);
		}

		// Check if E-TriAL-QL query is found as inline argument or as a
		// filename which contains the query.
		boolean file = false;
		if (args[0].equals("-f")) {
			file = true;
		}

		// SPARK cluster configuration.
		sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local");
		sparkConf.set("spark.sql.parquet.binaryAsString", "true");
		sparkConf.set("spark.sql.parquet.filterPushdown", "true");
		sparkConf.set("spark.executor.memory", "26g");
		sparkConf.set("spark.default.parallelism", "108");
		sparkConf.set("spark.sql.inMemoryColumnarStorage.compressed", "true");
		sparkConf.set("spark.sql.inMemoryColumnarStorage.batchSize", "26000");
		sparkConf.set("spark.sql.shuffle.partitions", args[2].toString());

		ctx = new JavaSparkContext(sparkConf);
		sqlContext = new SQLContext(ctx);

		runOnSPARK = true;

		String theQuery = args[0];

		if (file) {
			theQuery = readFromFile(args[1].toString());
		}

		// Extract predicates for partial load.
		String predicates[] = theQuery.split("\\bp1 = '\\b|\\bp2 = '\\b");
		for (String p : predicates) {
			String predicates2[] = p.split("'");
			if (!predicates2[0].contains(" ")) {
				takePredicates.add(predicates2[0]);
			}

		}

		// Split multiple E-TriAL-QL queries to single ones.
		String[] queryArray = theQuery.split(";");

		boolean theLastQuery = false;
		Dataset<Row> resultFrame = null;

		// Go through each E-TriAL-QL query one by one.
		if (queryArray.length == 1 && !queryArray[0].contains("LOAD")) {
			resultFrame = queryExecutor(queryArray[0] + ";", file, true, true);
		} else {
			for (int i = 0; i < queryArray.length; i++) {
				if (Configuration.compositeJoin) {
					resultFrame = queryExecutor(queryArray[i] + ";", file, theLastQuery, true);
				} else {

					if (queryArray[i].contains("LOAD")) {
						resultFrame = queryExecutor(queryArray[i] + ";", file, theLastQuery, false);

					} else if (i == 0) {
						queryExecutor(queryArray[i] + ";", file, theLastQuery, true);
					} else if (i == (queryArray.length - 1)) {
						if (queryArray[i].contains("STORE")) {
							resultFrame = queryExecutor(queryArray[i] + ";", file, theLastQuery, false);
						} else {
							theLastQuery = true;
							resultFrame = queryExecutor(queryArray[i] + ";", file, theLastQuery, false);
						}
					} else {
						queryExecutor(queryArray[i] + ";", file, theLastQuery, false);
					}
				}
			}
		}
		// Compose the queries, if necessary.
		if (Configuration.noFixedPoint) {
			ComposeQueries.composeMultipleJoinQueries();
			resultFrame = ResultStruct.finalResults;
		} else if (Configuration.compositeJoin) {
			ComposeQueries.composeMultipleJoinQueriesFixed();
			resultFrame = ResultStruct.finalResults;
		}
		long t1 = System.nanoTime();

		System.out.println("Elapsed time: " + (t1 - t0)/1000000000 + "s");

		int resultInt = (int) resultFrame.count();

		System.out.println("Result:" + resultInt);

		saveResults(resultFrame);

	}

	/**
	 * Load the initial Triplestore and run single SPARK query.
	 */
	private static Dataset<Row> queryExecutor(String singleQuery, boolean file, boolean theLastQuery,
			boolean theFirstQuery) throws Exception {

		if (theFirstQuery && notExecuted) {
			where = "SELECT subject, predicate, object FROM temp WHERE";
			for (String p : takePredicates) {
				where = where + " predicate='" + p + "' OR";
			}
			where = where.substring(0, where.length() - 3);

			Dataset<Row> schemaRDF = sqlContext.read()
					.parquet("/mnt/d/spark/"
							+ Configuration.initialTableName + "/");

			if (Configuration.noFixedPoint) {
				schemaRDF.createOrReplaceTempView("temp");
				schemaRDF = sqlContext.sql(where);
			}

			schemaRDF.cache().createOrReplaceTempView(Configuration.initialTableName);
			schemaRDF.count();

			notExecuted = false;
		}

		ParseTree tree = TriALQLParseQuery.parse(singleQuery, file);
		TriALQLClassListener extractor = new TriALQLClassListener();
		ParseTreeWalker.DEFAULT.walk(extractor, tree);

		int currentQueryCounter = QueryStruct.queryCounter - 1;
		Dataset<Row> resultFrame = null;

		if (singleQuery.contains("DROP")) {
			if (TriALQLClassListener.setProvenance) {
				Dataset<Row> schemaRDF = sqlContext.read()
						.parquet("/mnt/d/spark/" + QueryStruct.oldTableName.get(0));
				schemaRDF.drop("PROVENANCE").collect();

				deleteGraph(QueryStruct.oldTableName.get(0));
				schemaRDF.write().parquet("/mnt/d/spark/" + QueryStruct.oldTableName.get(0));
			} else {
				deleteGraph(QueryStruct.oldTableName.get(0));
			}

		} else if (QueryStruct.finalQuery.equals("LOAD")) {
			Dataset<Row> schemaRDF = sqlContext.read()
					.parquet("/mnt/d/spark/" + QueryStruct.oldTableName.get(0));
			schemaRDF.cache().createOrReplaceTempView(QueryStruct.oldTableName.get(0));
			if (TriALQLClassListener.isCached) {
				String makeHot = "SELECT COUNT(*) FROM " + QueryStruct.oldTableName.get(0);
				resultFrame = sqlContext.sql(makeHot);
			}

		} else if (!QueryStruct.finalQuery.equals("none")) {

			//resultFrame = sqlContext.sql("SHOW TABLES");
			//saveResults(resultFrame);

			System.out.println(QueryStruct.baseQuery.get(currentQueryCounter));
			resultFrame = sqlContext.sql(QueryStruct.baseQuery.get(currentQueryCounter));
			resultFrame.createOrReplaceTempView(QueryStruct.newTableName.get(currentQueryCounter));

			if (singleQuery.contains("STORE")) {
				resultFrame.write().parquet(
						"/mnt/d/spark/" + QueryStruct.newTableName.get(currentQueryCounter));
			}
		} else {
			resultFrame = ResultStruct.finalResults;
			resultFrame.cache().createOrReplaceTempView(QueryStruct.newTableName.get(currentQueryCounter));
			return  resultFrame;
		}
		return resultFrame;
	}

	/**
	 * Save the results and SPARK queries to the local system.
	 * 
	 * @param resultFrame
	 * @throws Exception
	 */
	private static void saveResults(Dataset<Row> resultFrame) throws Exception {

		if (Configuration.saveToFiles) {
			try {
				Row[] results = resultFrame.collectAsList().toArray(new Row[0]);
				PrintWriter writer = new PrintWriter("/mnt/d/spark/results/result.txt", "UTF-8");
				for (Row r : results) {
					String result = "";
					for (int i = 0; i < r.length(); i++) {
						result += r.get(i) + "\t";
					}
					writer.println(result);
				}
				writer.close();
			} catch (Exception e) {
				System.err.println(e);
			}
		}

		ctx.close();

		PrintWriter writer = new PrintWriter("/mnt/d/spark/results/queries.txt", "UTF-8");
		for (String Query : QueryStruct.baseQuery) {
			writer.println(Query + "\n");
		}
		writer.close();

		if (Configuration.SemNaive) {
			AppSpark.sqlContext.sql("DROP TABLE IF EXISTS deltaP");
			deleteGraph("deltaP1");
			deleteGraph("deltaP2");
			deleteGraph("deltaP3");
			deleteGraph("deltaP4");
			deleteGraph("deltaP5");
			deleteGraph("deltaP6");
			deleteGraph("deltaP7");
			deleteGraph("deltaP8");
			deleteGraph("deltaPA8");
		}

	}

	/**
	 * Delete triplestore from HDFS.
	 * 
	 * @param graphName
	 * @throws Exception
	 */
	public static void deleteGraph(String graphName) throws Exception {
		FileSystem fs = FileSystem.get(new URI(""), new org.apache.hadoop.conf.Configuration());
		fs.delete(new Path("/mnt/d/spark/" + graphName), true);

	}

	/**
	 * Read query from file.
	 * 
	 * @param thePath
	 * @return
	 * @throws IOException
	 */
	public static String readFromFile(String thePath) throws IOException {
		List<String> allLines = Files.readAllLines(Paths.get(thePath));

		String query = "";
		for (String line : allLines) {
			query += line;
		}
		return query;
	}

	/**
	 * Append result number to file.
	 * 
	 * @param Result
	 * @throws IOException
	 */
	public static void writeResult(String Result) throws IOException {
		Files.write(Paths.get("Results/time.txt"), Result.getBytes(), StandardOpenOption.APPEND);
	}

}
