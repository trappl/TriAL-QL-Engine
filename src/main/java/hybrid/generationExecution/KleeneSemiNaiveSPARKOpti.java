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

package hybrid.generationExecution;

import data.structures.QueryStruct;
import data.structures.ResultStruct;
import executor.AppSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
@SuppressWarnings("Duplicates")
public class KleeneSemiNaiveSPARKOpti {
	public static String finalQuery;
	public static String createTableQuery;
	public static String baseQuery = "";
	static long numberOfLines;
	static String whereExp = "";

	/**
	 * Semi-naive SPARK implementation of Transitive closure.
	 * 
	 * @param oldTableName
	 * @param newTableName
	 * @param joinOnExpression
	 * @param kleeneType
	 * @param selectionPart
	 */
	public static void CreateQuery(String[] oldTableName, String newTableName, ArrayList<String> joinOnExpression,
			String kleeneType, String[] selectionPart) {
		String temporaryQuery;
		String tableShortForm = oldTableName[0].substring(0, 2);
		String sel1, sel2, sel3, sel1l, sel2l, sel3l;
		String join = "";
		String topQueryPart = "";
		String union = "";

		Dataset<Row> result = AppSpark.sqlContext.sql("SELECT subject AS subject, predicate AS predicate, object AS object, 0 AS iter FROM " + oldTableName[0]);
		result.write().partitionBy("iter").format("parquet").mode(SaveMode.Overwrite).saveAsTable("deltaP");

		int stepCounter = 0;
		numberOfLines = 1;
		System.err.println("JOIN:");
		System.err.println(Arrays.toString(joinOnExpression.toArray()));
		while (numberOfLines > 0) {
			stepCounter++;

			String cTableShort = "deltaP";
			whereExp = " WHERE deltaP.iter="+ (stepCounter-1);

			if (selectionPart[0].equals("1")) {
				sel1 = cTableShort + "." + selectionPart[1];
				sel1l = tableShortForm + "1" + "." + selectionPart[1];
			} else {
				sel1 = tableShortForm + "1" + "." + selectionPart[1];
				sel1l = cTableShort + "." + selectionPart[1];
			}

			if (selectionPart[2].equals("1")) {
				sel2 = cTableShort + "." + selectionPart[3];
				sel2l = tableShortForm + "1" + "." + selectionPart[3];
			} else {
				sel2 = tableShortForm + "1" + "." + selectionPart[3];
				sel2l = cTableShort + "." + selectionPart[3];
			}

			if (selectionPart[4].equals("1")) {
				sel3 = cTableShort + "." + selectionPart[5];
				sel3l = tableShortForm + "1" + "." + selectionPart[5];
			} else {
				sel3 = tableShortForm + "1" + "." + selectionPart[5];
				sel3l = cTableShort + "." + selectionPart[5];
			}

			if (kleeneType.equals("right")) {
				topQueryPart = "SELECT DISTINCT " + sel1 + " AS subject, " + sel2 + " AS predicate, " + sel3
						+ " AS object" + " FROM deltaP JOIN " + oldTableName[0] + " " + tableShortForm
						+ 1 + " ON ";

				for (int k = 0; k < joinOnExpression.size(); k = k + 3) {
					if (k > 0) {
						join = join + " AND ";
					}

					if (joinOnExpression.get(k).toString().substring(2, 3).equals("1")) {
						join = join + " " + cTableShort + joinOnExpression.get(k).toString().substring(3);
					} else {
						join = join + " " + tableShortForm + 1 + joinOnExpression.get(k).toString().substring(3);
					}

					join = join + " " + joinOnExpression.get(k + 1) + " ";

					if (joinOnExpression.get(k + 2).toString().substring(2, 3).equals("1")) {
						join = join + " " + cTableShort + joinOnExpression.get(k + 2).toString().substring(3);
					} else {
						join = join + " " + tableShortForm + 1 + joinOnExpression.get(k + 2).toString().substring(3);
					}
				}
			} else if (kleeneType.equals("left")) {
				topQueryPart = "SELECT DISTINCT " + sel1l + " AS subject, " + sel2l + " AS predicate, " + sel3l
						+ " AS object" + " FROM " + oldTableName[0] + " " + tableShortForm + 1 + " JOIN deltaP ON ";

				for (int k = 0; k < joinOnExpression.size(); k = k + 3) {
					if (k > 0) {
						join = join + " AND ";
					}

					if (joinOnExpression.get(k).toString().substring(2, 3).equals("1")) {
						join = join + " " + tableShortForm + 1 + joinOnExpression.get(k).toString().substring(3);
					} else {
						join = join + " " + cTableShort + joinOnExpression.get(k).toString().substring(3);
					}

					join = join + " " + joinOnExpression.get(k + 1) + " ";

					if (joinOnExpression.get(k + 2).toString().substring(2, 3).equals("1")) {
						join = join + " " + tableShortForm + 1 + joinOnExpression.get(k + 2).toString().substring(3);
					} else {
						join = join + " " + cTableShort + joinOnExpression.get(k + 2).toString().substring(3);
					}
				}

			}

			// GENERATE new triples
			System.err.println("top: "+ topQueryPart);
            System.err.println("join"+ join);
			System.err.println("where"+ whereExp);
			Dataset<Row> resultFrame1 = AppSpark.sqlContext.sql(topQueryPart + join + whereExp);
			resultFrame1.cache().createOrReplaceTempView("tmp");
			//resultFrame1.show();

			baseQuery = baseQuery + topQueryPart + join + whereExp + "\n";

			// GET triples from temp which are not in deltaPA and SAVE it to deltaP
			temporaryQuery = "SELECT tmp.subject AS subject, tmp.predicate AS predicate,"
					+ " tmp.object AS object FROM tmp LEFT JOIN deltaP "
					+ " ON tmp.subject = deltaP.subject AND tmp.predicate = deltaP.predicate"
					+ " AND tmp.object = deltaP.object WHERE deltaP.predicate IS NULL";

			baseQuery = baseQuery + temporaryQuery + "\n";
			AppSpark.sqlContext.sql("INSERT INTO deltaP partition(iter=" + stepCounter + ") " + temporaryQuery);
			String resultsChecking = "SELECT COUNT(*) AS count FROM deltaP WHERE iter=" + stepCounter;
			numberOfLines = AppSpark.sqlContext.sql(resultsChecking).collectAsList().get(0).getLong(0);

			join = "";

			AppSpark.sqlContext.dropTempTable("tmp");

		}

		System.out.println("Loop Finished");

		Dataset<Row> resultFrame3 = AppSpark.sqlContext.sql("SELECT subject, predicate, object FROM deltaP");
		baseQuery = baseQuery + union + "\n";

		QueryStruct.fillStructure(oldTableName, newTableName, baseQuery, "none", "none");
		ResultStruct.fillStructureSpark(resultFrame3);
	}

}
