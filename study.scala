
//input parameters
//File from energy distribution in cnmc file
val cnmc_file = "consumptions.csv"
//File from  https://re.jrc.ec.europa.eu/pvg_tools/es/tools.html#api_5.3
val production_file = "production.csv"

val consumption = spark.read.option("delimiter",";").option("header",true).csv(cnmc_file)

val production = spark.read.text(production_file).
                      filter(col("value").contains(",") &&  size(split(col("value"), ",")) >= 3 && !col("value").contains("time")).
                      withColumn("split_col", split(
                        col("value"), ",")).select(
                          col("split_col").getItem(0).alias("time"),
                          col("split_col").getItem(1).alias("p"),
                          col("split_col").getItem(2).alias("g_i"),
                          col("split_col").getItem(3).alias("h_sun"),
                          col("split_col").getItem(4).alias("t2m"),
                          col("split_col").getItem(5).alias("ws10m"),
                          col("split_col").getItem(6).alias("int")).
                      withColumn("fecha",to_date(substring(col("time"),1,8),"yyyyMMdd")).
                      withColumn("hora",substring(col("time"),10,2).cast("int"))

