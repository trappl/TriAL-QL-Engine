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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import data.structures.Configuration;
import data.structures.QueryStruct;
import data.structures.ResultStruct;
import executor.AppSpark;

public class KleeneSemiNaiveSPARK {
	public static String finalQuery;
	public static String createTableQuery;
	public static String baseQuery = "";
	static ResultSet results = null;
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
	 * @param kleeneDepth1
	 * @param heuristicTable
	 */
	public static void CreateQuery(String[] oldTableName, String newTableName, ArrayList<String> joinOnExpression,
			String kleeneType, String[] selectionPart, int kleeneDepth1, String heuristicTable) {
		String temporaryQuery;
		String tableShortForm = oldTableName[0].substring(0, 2);
		String currentTableName = oldTableName[0];
		if (!heuristicTable.equals(""))
			currentTableName = "deltaP0";
		String sel1, sel2, sel3, sel1l, sel2l, sel3l;
		String join = "";
		String topQueryPart = "";
		String union = "";

		int stepCounter = 0;
		numberOfLines = 1;
		while (numberOfLines > 0) {
		//for (int z = 1; z <= 8; z++) {
			stepCounter++;

			String cTableShort = currentTableName;

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
						+ " AS object" + " FROM " + currentTableName + " JOIN " + oldTableName[0] + " " + tableShortForm
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
						+ " AS object" + " FROM " + oldTableName[0] + " " + tableShortForm + 1 + " JOIN "
						+ currentTableName + " ON ";

				System.err.println("join-part: "+ Arrays.toString(joinOnExpression.toArray()));
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

			System.err.println("top: "+ topQueryPart);
            System.err.println("join"+ join);
			System.err.println("where"+ whereExp);
			Dataset<Row> resultFrame1 = AppSpark.sqlContext.sql(topQueryPart + join + whereExp);
			resultFrame1.cache().createOrReplaceTempView("tmp");
			resultFrame1.show();

			baseQuery = baseQuery + topQueryPart + join + whereExp + "\n";

			if (stepCounter == 1) {
				union = " SELECT subject AS subject, predicate AS predicate, object AS object" + " FROM "
						+ oldTableName[0];

			} else {
				union = " SELECT subject, predicate, object FROM deltaPA" + (stepCounter - 1)
						+ " UNION ALL SELECT subject, predicate, object FROM deltaP" + (stepCounter - 1);
			}
			Dataset<Row> deltaPAFrame = AppSpark.sqlContext.sql(union);
			deltaPAFrame.cache().createOrReplaceTempView("deltaPA" + stepCounter);
			System.out.println("PA:");
			deltaPAFrame.show();

			temporaryQuery = "" + " SELECT tmp.subject AS subject, tmp.predicate AS predicate,"
					+ " tmp.object AS object FROM tmp LEFT JOIN deltaPA" + stepCounter + " AS MyTable1 "
					+ " ON tmp.subject = MyTable1.subject AND tmp.predicate = MyTable1.predicate"
					+ " AND tmp.object = MyTable1.object " + " WHERE MyTable1.predicate IS NULL";

			baseQuery = baseQuery + temporaryQuery + "\n";
			Dataset<Row> resultFrame2 = AppSpark.sqlContext.sql(temporaryQuery);
			resultFrame2.cache().createOrReplaceTempView("deltaP" + stepCounter);
			System.out.println("P:");
			resultFrame2.show();
			numberOfLines = resultFrame2.count();

			currentTableName = "deltaP" + stepCounter;
			join = "";

			AppSpark.sqlContext.dropTempTable("tmp");

		}

		System.out.println("Loop Finished");

		if (!heuristicTable.equals("")) {
			union = union + " UNION ALL SELECT * FROM deltaP" + stepCounter + " UNION SELECT * FROM temp1"
					+ " UNION SELECT * FROM temp2";
		}
		if (kleeneDepth1 == -10) {
			union = union.substring(90);
		}

		Dataset<Row> resultFrame3 = AppSpark.sqlContext.sql("SELECT * FROM deltaPA" + stepCounter);
		baseQuery = baseQuery + union + "\n";

		QueryStruct.fillStructure(oldTableName, newTableName, baseQuery, "none", "none");
		ResultStruct.fillStructureSpark(resultFrame3);
	}

}
