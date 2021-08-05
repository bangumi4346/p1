import org.apache.spark.sql.SparkSession

object p1 {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  val spark = SparkSession
    .builder
    .appName("p1")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  println("created spark session")
  spark.sparkContext.setLogLevel("WARN")

  spark.sql("CREATE TABLE IF NOT EXISTS brancha (blend string, branch string) row format delimited fields terminated by ','")
  spark.sql("CREATE TABLE IF NOT EXISTS branchb (blend string, branch string) row format delimited fields terminated by ','")
  spark.sql("CREATE TABLE IF NOT EXISTS branchc (blend string, branch string) row format delimited fields terminated by ','")
  spark.sql("CREATE TABLE IF NOT EXISTS conscounta (blend string, consumers int) row format delimited fields terminated by ','")
  spark.sql("CREATE TABLE IF NOT EXISTS conscountb (blend string, consumers int) row format delimited fields terminated by ','")
  spark.sql("CREATE TABLE IF NOT EXISTS conscountc (blend string, consumers int) row format delimited fields terminated by ','")
  spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE brancha")
  spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE branchb")
  spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE branchc")
  spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE conscounta")
  spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE conscountb")
  spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE conscountc")


  val join_a = spark.sql("select conscounta.blend, sum(consumers) total from conscounta join brancha on conscounta.blend = brancha.blend where branch = 'Branch1' group by conscounta.blend order by conscounta.blend asc")
  val join_b = spark.sql("select conscountb.blend, sum(consumers) total from conscountb join branchb on conscountb.blend = branchb.blend where branch = 'Branch1' group by conscountb.blend order by conscountb.blend asc ")
  val join_c = spark.sql("select conscountc.blend, sum(consumers) total from conscountc join branchc on conscountc.blend = branchc.blend where branch = 'Branch1' group by conscountc.blend order by conscountc.blend asc")
  val b1_blend_sold = join_a.union(join_b).union(join_c)
  b1_blend_sold.createOrReplaceTempView("b1_blend_sold")

  val branches = spark.sql("select * from brancha union all select * from branchb union all select * from branchc")
  branches.createOrReplaceTempView("branches")

  def scenario_1: Unit ={
    spark.sql("select sum(total) totalcount from b1_blend_sold").show()
  }

  def scenario_2(mode:String): Unit ={
    if(mode == "min"){
      spark.sql("select min(total) min from b1_blend_sold").show()
    }
    else if(mode == "max"){
      spark.sql("select max(total) max from b1_blend_sold").show()
    }
  }

  def scenario_3: Unit ={
    spark.sql("select distinct blend from branches where branch = \"Branch10\" or branch = \"Branch8\" or branch = \"Branch1\"").show()
  }

  def scenario_4(mode:String): Unit ={
    if(mode == "partition"){
      branches.write.partitionBy("branch")
    }
    else if(mode == "view"){
      val view = spark.sql("select distinct blend from branches where branch = \"Branch10\" or branch = \"Branch8\" or branch = \"Branch1\"")
      view.createOrReplaceTempView("scenario_4_view")
      view.show()
    }
  }

  def scenario_5(table:String, column:String, comment:String): Unit ={
    println("Original:")
    spark.sql(s"describe $table").show()
    spark.sql(s"alter table $table change $column $column string comment '$comment'")
    println("After:")
    spark.sql(s"describe $table").show()
  }

  def scenario_6(table:String, n:Int): Unit ={
    spark.sql("drop table if exists lim")
    val lim = spark.sql(s"select * from $table sort by blend asc limit $n")
    lim.createOrReplaceTempView("lim")
    spark.sql("select max(blend) from lim").show()
    //spark.sql("select * from b1_blend_sold").show()
    spark.sql("select * from b1_blend_sold where blend not in (select max(blend) from lim)").show()
  }

  def main(args: Array[String]): Unit = {

    //scenario_1

    //scenario_2("min")
    //scenario_2("max")

    //scenario_3

    //scenario_4("partition")
    //scenario_4("view")

    //scenario_5("conscounta", "blend", "this is the comment")

    // PPT:
    // scenario_6("b1_blend_sold",5)

    // DEMO:
    scenario_6("b1_blend_sold",4)

  }
}