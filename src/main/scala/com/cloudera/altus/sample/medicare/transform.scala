/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.altus.sample.medicare

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.avg

object transform {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Medicare Analysis")
//    val conf = new SparkConf().setAppName("Medicare Analysis").setMaster("local")
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)

    import sqc.implicits._

    val demoSourceDirS3 = args(0)
    val demoOutDirS3 = args(1)

    val hosp_file = sc.textFile(demoSourceDirS3 + "/hospitals.txt")
    val read_file = sc.textFile(demoSourceDirS3 + "/readmissionsDeath.txt")
    val gdp_file = sc.textFile(demoSourceDirS3 + "/gdp.txt")

    val hosp_header = hosp_file.first()
    val read_header = read_file.first()
    val gdp_header = gdp_file.first()

    val hosp_f = hosp_file.filter(line => !line.contains(hosp_header)).cache()
    val read_f = read_file.filter(line => !line.contains(read_header)).cache()
    val gdp_f = gdp_file.filter(line => !line.contains(gdp_header)).cache()

    val hosp_split = hosp_f.map(x => x.split("\t"))
    val hosp_df = hosp_split.map(x => (x(0),x(1),x(2),x(3), x(4),x(5),x(6),x(7),x(8),x(9),x(10))).toDF()

    val read_split = read_f.map(x => x.split("\t"))
    val read_df = read_split.map(x => (x(0),x(1),x(2),x(3), x(4),x(5),
      x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13), x(14),x(15),x(16),x(17))).toDF()

    val gdp_split = gdp_f.map(x => x.split("\t"))
    val gdp_df = gdp_split.map(x => (x(0),x(1),x(2),x(3), x(4),x(5))).toDF()

    val a = hosp_df.first()
    val b = read_df.first()
    val c = gdp_df.first()

    val newNames = Seq("ProviderID", "HospitalName","Address","City", "State", "ZIPCode",
                       "CountyName", "PhoneNumber", "HospitalType",  "HospitalOwnership",
                       "EmergencyServices")
    val hosp_renamed = hosp_df.toDF(newNames: _*)

    val newNames1 = Seq("ProviderID", "HospitalName", "Address", "City",
                        "State", "ZIPCode","CountyName", "PhoneNumber",
                        "MeasureName", "MeasureID","ComparedToNational",
                        "Denominator", "Score", "LowerEstimate",
                        "HigherEstimate", "Footnote", "MeasureStartDate",
                        "MeasureEndDate")

    val read_renamed = read_df.toDF(newNames1: _*).filter($"Score" !== "Not Available")

    val newNames2 = Seq("State", "GDP2015", "GDP2012", "2011", "GDP2010", "GDP2009")
    val gdp_renamed = gdp_df.toDF(newNames2: _*)

    val hosp_red = hosp_renamed.select("ProviderID","State","HospitalType")
    val read_red = read_renamed.select("ProviderID","MeasureName", "Score")
    val gdp_red = gdp_renamed.select("State", "GDP2012")

    val hosp_read = hosp_red.join(read_red,Seq("ProviderID"))
    val hosp_read_gdp = hosp_read.join(gdp_red, Seq("State"))
    val hrg = hosp_read_gdp.groupBy("State","MeasureName","HospitalType")
      .agg($"State", $"MeasureName",$"HospitalType", avg("Score")).cache()
    var result = hrg.join(gdp_red, Seq("State")).toDF()

    val newNames3 = Seq("_0", "_1","_2","_3", "_4", "_5", "_6")
    val result_tmp = result.toDF(newNames3: _*)
    val result_tmp2 = result_tmp.select("_0","_1","_2","_5","_6")

    val newNames4 = Seq("State", "Condition","HospitalType","avgScore", "GDP")
    val result_fin = result_tmp2.toDF(newNames4: _*)

    println(result_fin.printSchema())
    println(result_fin.first())

    result_fin.write.format("com.databricks.spark.csv")
      .option("header", "false")
      .save(demoOutDirS3)
  }
}
