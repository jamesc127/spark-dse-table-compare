import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import org.apache.spark.sql.types.{ArrayType, DecimalType, StructType}

object TableCompare {
  def main(args: Array[String]): Unit = {
    val spark               = SparkSession.builder.appName("Table Compare").master("local[1]").getOrCreate()
    val config              = ConfigFactory.load()
    val master_table        = config.getString("master_table.table")
    val master_keyspace     = config.getString("master_table.keyspace")
    val compare_table       = config.getString("compare_table.table")
    val compare_keyspace    = config.getString("compare_table.keyspace")
    val columns_to_drop     = config.getStringList("exclude_columns").asScala
    val primary_key_columns = config.getStringList("primary_key_columns").asScala

    def updateColumnsToString(df:DataFrame,columnIt:Iterator[String]):DataFrame = {
      def dfHelper(dFrame:DataFrame,col:String):DataFrame = {
        if (columnIt.isEmpty) dFrame
        else dfHelper(dFrame.withColumn(col,concat_ws(", ",dFrame(col))),columnIt.next())
      }
      dfHelper(df,columnIt.next())
    }

    def matchColumnTypes(df:DataFrame,col:String): DataFrame = {
      df.schema(col).dataType match {
        case ArrayType(StructType(_),_) => df.withColumn(col,hash(df(col))) //hashes a frozen<list<udt>> before comparing
        case DecimalType() => df.withColumn(col,df(col).cast(DecimalType(10,2))) //decimal types get two digits after the decimal point
        case _ => df
      }
    }

    def updateTypes(df:DataFrame,columnIt:Iterator[String]):DataFrame = {
      def dfHelper(dFrame:DataFrame,col:String):DataFrame = {
        if (columnIt.isEmpty) dFrame
        else dfHelper(matchColumnTypes(dFrame,col),columnIt.next())
      }
      dfHelper(df,columnIt.next())
    }

    def sortCollections(df:DataFrame,columnIt:Iterator[String]):DataFrame = {
      def dfHelper(df:DataFrame, mColumn:String):DataFrame = {
        def matchCase(df:DataFrame): DataFrame = {
          df.schema(mColumn).dataType match {
            case ArrayType(_,_) => df.withColumn(mColumn,sort_array(df(mColumn))) //ensures a collection of whatever type is sorted
            case _ => df
          }
        }
        if (columnIt.isEmpty) df
        else dfHelper(matchCase(df),columnIt.next())
      }
      dfHelper(df,columnIt.next())
    }

    val columns = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "columns", "keyspace" -> "system_schema")).load()
    val columnsDropped = columns.filter(!col("column_name").isin(columns_to_drop:_*))
    columnsDropped.createOrReplaceTempView("columns")

    val df = spark.sql(s"""
    SELECT concat('t1.', column_name, ' AS t1_', column_name, ', t2.', column_name, ' AS t2_', column_name, ',') AS select_clause_fields
    FROM columns
    WHERE keyspace_name = '$master_keyspace'
    AND table_name = '$master_table'
    """) //select clause sql

    val select_clause = df.select("select_clause_fields").rdd.collect.mkString.replace("[", "").replace("]"," ")
    val select_clause_trim = select_clause.substring(0,select_clause.length-2)

    val table1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> master_table, "keyspace" -> master_keyspace)).load()
    val table2 = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> compare_table, "keyspace" -> compare_keyspace)).load()

    val table1Drop = table1.drop(columns_to_drop:_*)
    val table2Drop = table2.drop(columns_to_drop:_*)

    val table1SortCollection = sortCollections(table1Drop,table1Drop.columns.toIterator)
    val table2SortCollection = sortCollections(table2Drop,table2Drop.columns.toIterator)

    val table1Hash = table1SortCollection.withColumn("hash",hash(table1Drop.columns.map(col):_*))
    val table2Hash = table2SortCollection.withColumn("hash",hash(table2Drop.columns.map(col):_*))

    table1Hash.createOrReplaceTempView("table1Hashed")
    table2Hash.createOrReplaceTempView("table2Hashed")

    val dfWithHashSelect = new StringBuilder
    for (j <- primary_key_columns){
      if (j == primary_key_columns.head) dfWithHashSelect.append(j)
      else dfWithHashSelect.append(s""", $j""")
    }

    val table1KeyAndHash = spark.sql(s"""SELECT $dfWithHashSelect, hash FROM table1Hashed""")
    val table2KeyAndHash = spark.sql(s"""SELECT $dfWithHashSelect, hash FROM table2Hashed""")

    table1KeyAndHash.createOrReplaceTempView("table1KeyAndHash")
    table2KeyAndHash.createOrReplaceTempView("table2KeyAndHash")

    val Table1ExceptTable2 = spark.sql("SELECT * FROM table1KeyAndHash EXCEPT SELECT * FROM table2KeyAndHash")
    val Table2ExceptTable1 = spark.sql("SELECT * FROM table2KeyAndHash EXCEPT SELECT * FROM table1KeyAndHash")

    val table1Rejoined = Table1ExceptTable2.join(table1Drop,primary_key_columns,"inner")
    val table2Rejoined = Table2ExceptTable1.join(table2Drop,primary_key_columns,"inner")

    table1Rejoined.createOrReplaceTempView("t1")
    table2Rejoined.createOrReplaceTempView("t2")

    val resultsJoinConditions = new StringBuilder
    for (j <- primary_key_columns){
      if (j == primary_key_columns.head) resultsJoinConditions.append(s"""t1.$j = t2.$j""")
      else resultsJoinConditions.append(s""" AND t1.$j = t2.$j""")
    }

    val results = spark.sql(s"""SELECT $select_clause_trim FROM t1 FULL OUTER JOIN t2 ON $resultsJoinConditions""")
    val resultsString = updateColumnsToString(updateTypes(results,results.columns.toIterator),results.columns.toIterator)
    resultsString.coalesce(1).write.option("header","true").option("delimiter", "\t").option("quote", "\u0000").csv(config.getString("csv_path"))
  }
}