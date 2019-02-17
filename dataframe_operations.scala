
//Using transform 
//https://medium.com/@mrpowers/chaining-custom-dataframe-transformations-in-spark-a39e315f903c

def model()(df: DataFrame): DataFrame = {
  df
    .transform(withGreeting())
    .transform(withFarewell())
}

extractDF.transform(model())

