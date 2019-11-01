# SparkML

This repository contains an example of a prediction model implemented in scala using spark libraries.

In order to run the example, download the git and use an IDE like IntelliJ with auto-import enabled
to build the project based on the sbt file.

The parameters used in the main class are:

  - **File_Path**: Path of the input data, the data used in this example is the year 2008 .csv from here http://stat-computing.org/dataexpo/2009/2008.csv.bz2
  - **Model_Path**: Path where the model will be saved (In case **Mode** is "Train") or from where the model will be read (In case **Mode** is "Test").
  - **Mode**: In case it is equal to "Train" it will train a new model. If it is equal to "Test" it will read a model and evaluate the test data.
  
Once the project is built and the data downloaded and saved in a Data folder, open a sbt console from the git root and execute the following for training a new model:

```

run .//Data//2008.csv .//Models/new_model Train

```

And for testing the model already trained:

```
run .//Data//2008.csv .//Models//new_model Test
```

This example can be modified to test others models from the different libraries like Random Forests or Gradient Boosted Trees (GBT).
