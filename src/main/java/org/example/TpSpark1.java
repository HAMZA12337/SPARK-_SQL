package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
public class TpSpark1 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession ss=SparkSession.builder().master("local[*]").appName("first exercise").getOrCreate();
        Map<String,String> options=new HashMap<>();
        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/db_hospital");
        options.put("user","root");
        options.put("password","");


        ///////////////////////  First version

//        String query1="SELECT date_consultation, COUNT(*) AS nombre_consultations \n" +
//                "FROM consultations \n" +
//                "GROUP BY date_consultation;";
//        String query2="SELECT med.nom, med.prenom, COUNT(*) AS nombre_consultations FROM consultations cons JOIN medecins med ON cons.medecin_Id = med.id";
//
//        String query3="SELECT med.nom, med.prenom, COUNT(*) AS nombre_consultations FROM medecins med JOIN consultations cons ON med.id=cons.medecin_id JOIN patients pat ON pat.id=cons.patient_id;";
//
//        Dataset<Row> dfEmp = ss.read().format("jdbc")
//                .options(options)
//                //.option("dbtable", "EMPLOYES")
//                .option("query","SELECT med.nom, med.prenom, COUNT(*) AS nombre_consultations FROM consultations cons JOIN medecins med ON cons.medecin_Id = med.id")
//                .load();
//        dfEmp.printSchema();
//        dfEmp.show();


        System.out.println("------------------------ Query 1 --------------------------------");
        // first query
        Dataset<Row> df1 = ss.read().format("jdbc").options(options).option("dbtable","consultations").load();
        df1.select("DATE_CONSULTATION").groupBy("DATE_CONSULTATION").count().show();

        // second query
        System.out.println("---------------------------Query 2--------------------------------");
        Dataset<Row> df2_1 = ss.read().format("jdbc").options(options).option("dbtable","consultations").load();
        Dataset<Row> df2_2 = ss.read().format("jdbc").options(options).option("dbtable","medecins").load();

        Dataset<Row> f2_merge=df2_1.join(df2_2);
        f2_merge.printSchema();
        f2_merge.show();

        f2_merge.select("NOM","PRENOM").groupBy("NOM","PRENOM").count().show();

        System.out.println("---------------------------Query 3--------------------------------");
        Dataset<Row> df3_1 = ss.read().format("jdbc").options(options).option("dbtable","medecins").load();
        Dataset<Row> df3_2 = ss.read().format("jdbc").options(options).option("dbtable","consultations").load();
        Dataset<Row> f3_merge=df3_1.join(df3_2);
        f2_merge.select("NOM","PRENOM").groupBy("NOM","PRENOM").agg(countDistinct("id_patient").as("nombre_patients")).show();

        df3.select("NOM","PRENOM","ID_PATIENT").groupBy("NOM","PRENOM").agg(countDistinct("ID_PATIENT")).show();

        Dataset<Row> dfJoin1 = dfMed.join(dfCon, dfCon.col("id_medecin").equalTo(dfMed.col("id")), "inner");
        dfJoin1.groupBy("nom", "prenom").agg(countDistinct("id_patient").as("nombre_patients")).show();


    }




}
