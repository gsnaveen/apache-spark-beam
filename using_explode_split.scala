var df = List(("ASR 1000, C3850"),("C2960, C3850")).toDF("product_family")
df = df.withColumn("myprod", explode(split(translate($"product_family"," ",""),",")))
