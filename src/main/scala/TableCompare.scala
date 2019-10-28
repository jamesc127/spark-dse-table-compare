import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import org.apache.spark.sql.types.DecimalType

object TableCompare {
  def main(args: Array[String]): Unit = {
    val spark               = SparkSession.builder.appName("Table Compare").getOrCreate()
    val config              = ConfigFactory.load()
    val master_table        = config.getString("master_table.table")
    val master_keyspace     = config.getString("master_table.keyspace")
    val compare_table       = config.getString("compare_table.table")
    val compare_keyspace    = config.getString("compare_table.keyspace")
    val primary_join        = config.getString("join_column.primary")
    val clustering_join1    = config.getString("join_column.clustering1")
    val clustering_join2    = config.getString("join_column.clustering2")
    val clustering_join3    = config.getString("join_column.clustering3")
    val clustering_join4    = config.getString("join_column.clustering4")
    val columns_to_drop     = config.getStringList("exclude_columns.column_list").asScala

    val columns = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> config.getString("system_table.table"), "keyspace" -> config.getString("system_table.keyspace"))).load()
    columns.filter(!col("column_name").isin(columns_to_drop:_*)).createOrReplaceTempView("columns")

    val decimalColumnsDf = spark.sql(s"""SELECT column_name FROM columns WHERE keyspace_name = '$master_keyspace' AND table_name = '$master_table' AND type = 'decimal'""")
    val decimalColumns = decimalColumnsDf.select("column_name").rdd.map(r => r(0)).collect().toList.asInstanceOf[List[String]]

    val df = spark.sql(s"""
    SELECT concat('t1.', column_name, ' AS t1_', column_name, ', t2.', column_name, ' AS t2_', column_name, ',') AS select_clause_fields
    FROM columns
    WHERE keyspace_name = '$master_keyspace'
    AND table_name = '$master_table'
    """)

    val select_clause = df.select("select_clause_fields").rdd.collect.mkString.replace("[", "").replace("]"," ")
    val select_clause_trim = select_clause.substring(0,select_clause.length-2)

    val table1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> master_table, "keyspace" -> master_keyspace)).load()
    val table2 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> compare_table, "keyspace" -> compare_keyspace)).load()

    def updateDecimalTypes(df:DataFrame,columnIt:Iterator[String]):DataFrame = {
      def dfHelper(dFrame:DataFrame,col:String):DataFrame = {
        if (columnIt.isEmpty) dFrame
        else dfHelper(dFrame.withColumn(col,dFrame(col).cast(DecimalType(10,2))),columnIt.next())
      }
      dfHelper(df,columnIt.next())
    }

    val table1Updated = updateDecimalTypes(table1,decimalColumns.toIterator)
    val table2Updated = updateDecimalTypes(table2,decimalColumns.toIterator)

    table1Updated.createOrReplaceTempView("table1")
    table2Updated.createOrReplaceTempView("table2")

    spark.sql("SELECT * FROM table1 EXCEPT SELECT * FROM table2").createOrReplaceTempView("t1")
    spark.sql("SELECT * FROM table2 EXCEPT SELECT * FROM table1").createOrReplaceTempView("t2")
    println(spark.sql("SELECT * FROM t1").show(500,false).toString)
    println(spark.sql("SELECT * FROM t2").show(500,false).toString)

    val clustering1Join = if (clustering_join1 != "null" && clustering_join1 != "") s""" AND t1.$clustering_join1 = t2.$clustering_join1""" else ""
    val clustering2Join = if (clustering_join2 != "null" && clustering_join2 != "") s""" AND t1.$clustering_join2 = t2.$clustering_join2""" else ""
    val clustering3Join = if (clustering_join3 != "null" && clustering_join3 != "") s""" AND t1.$clustering_join3 = t2.$clustering_join3""" else ""
    val clustering4Join = if (clustering_join4 != "null" && clustering_join4 != "") s""" AND t1.$clustering_join4 = t2.$clustering_join4""" else ""

    val results = spark.sql(s"""SELECT $select_clause_trim FROM t1 FULL OUTER JOIN t2 ON t1.$primary_join = t2.$primary_join$clustering1Join$clustering2Join$clustering3Join$clustering4Join""")

    def updateColumnTypes(df:DataFrame,columnIt:Iterator[String]):DataFrame = {
      def dfHelper(dFrame:DataFrame,col:String):DataFrame = {
        if (columnIt.isEmpty) dFrame
        else dfHelper(dFrame.withColumn(col,concat_ws(", ",dFrame(col))),columnIt.next())
      }
      dfHelper(df,columnIt.next())
    }

    val resultsString = updateColumnTypes(results,results.columns.toIterator)
    resultsString.coalesce(1).write.option("header","true").option("delimiter", "\t").option("quote", "\u0000").csv(config.getString("csv_path.output_path"))
  }
}