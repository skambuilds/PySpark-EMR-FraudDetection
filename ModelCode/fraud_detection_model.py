import re
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
from pyspark.sql.types import StringType
from pyspark.ml.feature import OneHotEncoderEstimator, OneHotEncoder, StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StandardScaler
from pyspark.sql.functions import udf
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession



if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ml-transactions")\
        .getOrCreate()

    # PARAMETERS    
    enableFeatSelection = True
    awsSourceLocation = True
    enableNormalization = False
    enableCompleteDF = True
    enableTargetStringConv = True
    enableStandardization = True
    enableClassWeights = False
    
    # CLASSIFIERS SELECTION    
    logReg = True
    decTree = True    
    
    # FILE LOCATIONS    
    if awsSourceLocation:
        bucket_name = 's3://spark-fraud-resources'
        train_ts_location = bucket_name + '/input/train_transaction.csv'
        train_id_location = bucket_name + '/input/train_identity.csv'	
        test_ts_location = bucket_name + '/input/test_transaction.csv'
        test_id_location = bucket_name + '/input/test_identity.csv'
    else:
        train_ts_location = "/FileStore/tables/train_transaction.csv"
        train_id_location = "/FileStore/tables/train_identity.csv"
        test_ts_location = "/FileStore/tables/test_transaction.csv"
        test_id_location = "/FileStore/tables/test_identity.csv"


    #### FEATURE SELECTION ####
    
    # FIRST 53 COLUMNS
    cols = ['TransactionID', 'isFraud', 'TransactionDT', 'TransactionAmt',
           'ProductCD', 'card1', 'card2', 'card3', 'card4', 'card5', 'card6',
           'addr1', 'addr2', 'dist1', 'dist2', 'P_emaildomain', 'R_emaildomain',
           'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11',
           'C12', 'C13', 'C14', 'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8',
           'D9', 'D10', 'D11', 'D12', 'D13', 'D14', 'D15', 'M1', 'M2', 'M3', 'M4',
           'M5', 'M6', 'M7', 'M8', 'M9']
    
    # V COLUMNS TO LOAD DECIDED BY CORRELATION EDA
    # https://www.kaggle.com/cdeotte/eda-for-columns-v-and-id
    v =  [1, 3, 4, 6, 8, 11]
    v += [13, 14, 17, 20, 23, 26, 27, 30]
    v += [36, 37, 40, 41, 44, 47, 48]
    v += [54, 56, 59, 62, 65, 67, 68, 70]
    v += [76, 78, 80, 82, 86, 88, 89, 91]
    
    v += [96, 98, 99, 104] 
    v += [107, 108, 111, 115, 117, 120, 121, 123] 
    v += [124, 127, 129, 130, 136]
    
    v += [138, 139, 142, 147, 156, 162]
    v += [165, 160, 166]
    v += [178, 176, 173, 182]
    v += [187, 203, 205, 207, 215]
    v += [169, 171, 175, 180, 185, 188, 198, 210, 209]
    v += [218, 223, 224, 226, 228, 229, 235]
    v += [240, 258, 257, 253, 252, 260, 261]
    v += [264, 266, 267, 274, 277]
    v += [220, 221, 234, 238, 250, 271]
    
    v += [294, 284, 285, 286, 291, 297] 
    v += [303, 305, 307, 309, 310, 320] 
    v += [281, 283, 289, 296, 301, 314]
    v += [332, 325, 335, 338] 
    
    cols += ['V'+str(x) for x in v]
    
    
    # LOAD TRAIN
    print("Train dataset loading...")
    train_ts = spark.read.csv(train_ts_location, header = True, inferSchema = True)
    if enableFeatSelection:    
        train_ts = train_ts.select(cols)
    train_id = spark.read.csv(train_id_location, header = True, inferSchema = True)
    
    train_df = train_ts.join(train_id, "TransactionID", how='left')
    print("Train dataset loading complete")
    
    # LOAD TEST
    print("Test dataset loading...")
    test_ts = spark.read.csv(test_ts_location, header = True, inferSchema = True)
    if enableFeatSelection:  
        cols.remove('isFraud')
        test_ts = test_ts.select(cols)
    test_id = spark.read.csv(test_id_location, header = True, inferSchema = True)
    
    test_df = test_ts.join(test_id, "TransactionID", how='left')
    print("Test dataset loading complete")
    
    if enableCompleteDF:
        train_ts.unpersist()
    train_id.unpersist()
    test_ts.unpersist()
    test_id.unpersist()
    
    # PRINT STATUS
    print("Train and test datasets shape:")
    print((train_df.count(), len(train_df.columns)))
    print((test_df.count(), len(test_df.columns)))    
	
    #***************************************************#

    print("Starting Feature engineering...")

    #### FEATURE ENGINEERING ####
   
    spark.conf.set("spark.sql.broadcastTimeout",  36000)
    
    
    # COLUMNS DEFINITION
    targetColumn = ["isFraud"];
    
    card_Columns  = [f'card{i}' for i in range(1, 7)];
    booleanColumns = ['M1','M2','M3','M5','M6','M7','M8','M9'];
    categoricalColumns = ["ProductCD", "P_emaildomain", "R_emaildomain", "addr1","addr2",'M4']+card_Columns+booleanColumns;
    
    intColumns = ["TransactionID", "TransactionDT"]; 
    
    
    doubleColumns = ["TransactionAmt", "dist1", "dist2"]
    doubleColumns_D = [f'D{i}' for i in range(1, 16)];
    doubleColumns_C = [f'C{i}' for i in range(1, 15)];
    
    if enableFeatSelection:
        doubleColumns_V = ['V'+str(x) for x in v]
    else:
        doubleColumns_V = [f'V{i}' for i in range(1, 340)]
    
    allDoubleColumns = doubleColumns+doubleColumns_D+doubleColumns_C+doubleColumns_V
    nonCategoricalColumns = intColumns+allDoubleColumns
    
    # SETTING COLUMNS OF TRAIN_IDENTITY
    if enableCompleteDF:
        doubleColumns_lowerids = [f'id_0{i}' for i in range(1, 9)];
        doubleColumns_higherids = ["id_10", "id_11"];
        allDoubleColumns += doubleColumns_lowerids+doubleColumns_higherids
        
        categoricalColumns_id = [f'id_{k}' for k in range(12, 38)];
        categoricalColumns_id.extend(["DeviceType", "DeviceInfo"])
        
        categoricalColumns += categoricalColumns_id
        nonCategoricalColumns += doubleColumns_lowerids+doubleColumns_higherids
      
    # SETTING NORMALIZED COLUMNS NAMING
    doubleColumns_n = ["TransactionAmt_norm", "dist1_norm", "dist2_norm"]
    doubleColumns_D_n = [f'D{i}_norm' for i in range(1, 16)];
    doubleColumns_C_n = [f'C{i}_norm' for i in range(1, 15)];
    
    if enableFeatSelection:
        doubleColumns_V_n = ['V'+str(x)+'_norm' for x in v]
    else:
        doubleColumns_V_n = [f'V{i}_norm' for i in range(1, 340)]
    
    allDoubleColumns_n = doubleColumns_n+doubleColumns_D_n+doubleColumns_C_n+doubleColumns_V_n
    nonCategoricalColumns_n = ["TransactionID"]+allDoubleColumns_n
    
    if enableCompleteDF:
        doubleColumns_lowerids_n = [f'id_0{i}_norm' for i in range(1, 9)];
        doubleColumns_higherids_n = ["id_10_norm", "id_11_norm"];
        allDoubleColumns_n += doubleColumns_lowerids_n+doubleColumns_higherids_n
        nonCategoricalColumns_n += doubleColumns_lowerids_n+doubleColumns_higherids_n
      
    # SETTING MODEL REFERENCE DATASET
    if enableCompleteDF:
        train_transaction = train_df
    else:
        train_transaction = train_ts
    
    # DATASOURCE ROWS COUNTER
    count = train_transaction.count()
    print ("train_transaction creation: %d" % count)
    
    # CATEGORICAL COLUMNS STRING CASTING
    for col in categoricalColumns:
        train_transaction = train_transaction.withColumn(col, train_transaction[col].cast(StringType()))
    
    # NULL VALUES SUBSTITUTION FOR CATEGORICAL COLUMNS
    for _col in categoricalColumns:
        train_transaction = train_transaction.withColumn(_col, when(train_transaction[_col].isNull(), 'none').otherwise(train_transaction[_col]))
    
    # NULL VALUES SUBSTITUTION FOR NON CATEGORICAL
    for _col in nonCategoricalColumns:
        train_transaction = train_transaction.withColumn(_col, when(train_transaction[_col].isNull(), 0).otherwise(train_transaction[_col]))
    
    # DOUBLE COLUMNS NORMALIZATION VIA ASSAMBLER
    if enableNormalization:
        assembler = VectorAssembler(inputCols=allDoubleColumns,outputCol="features")
        train_transaction = assembler.transform(train_transaction)
        
        MinMaxScalerizer=MinMaxScaler().setMin(0).setMax(10).setInputCol("features").setOutputCol("mmsfeatures")
        train_transaction = MinMaxScalerizer.fit(train_transaction).transform(train_transaction)
    
        ### DISASSEMBLER ###
        def extract(row):
            return (row.TransactionID, ) + tuple(row.mmsfeatures.toArray().tolist())  
        
        norm_df = train_transaction.rdd.map(extract).toDF(nonCategoricalColumns_n)
        
        ### SUPPORT COLUMNS ELIMIMINATION
        train_transaction = train_transaction.drop('features')
        train_transaction = train_transaction.drop('mmsfeatures')
    
        ### NON NORMALIZED COLUMNS ELIMINATION
        for col in allDoubleColumns:
            train_transaction = train_transaction.drop(col)
    
        ### ADDING NORMALIZED COLUMNS
        train_transaction = train_transaction.join(norm_df, "TransactionID")
      
        ### NON CATEGORICAL COLUMNS NAMING UPDATE
        nonCategoricalColumns = intColumns + allDoubleColumns_n
    
        #### SOSTITUZIONE VALORI BOOLEANI T F ####
        #for _col in booleanColumns:
            #train_transaction = train_transaction.withColumn(_col, when(train_transaction[_col] == 'T', 1).otherwise(0))
      
    cols = train_transaction.columns
    
    if enableTargetStringConv:
        trgStrConv_udf = udf(lambda val: "no" if val==0 else "yes", StringType())
        train_transaction=train_transaction.withColumn("isFraud", trgStrConv_udf('isFraud'))

    print("Feature engineering complete!")

    print("Starting hot encoding...")
    #### ONE HOT ENCODING ####
    stages = []
    
    for categoricalCol in categoricalColumns:
        stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
        stages += [stringIndexer, encoder]
    
    label_stringIdx = StringIndexer(inputCol = 'isFraud', outputCol = 'label')
    stages += [label_stringIdx]
    
    assemblerInputs = [c + "classVec" for c in categoricalColumns] + nonCategoricalColumns
    if enableStandardization:
        assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="vectorized_features")
        stages += [assembler]
        scaler = StandardScaler(inputCol="vectorized_features", outputCol="features")
        stages += [scaler]
    else:
        assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
        stages += [assembler]
    
    
    pipeline = Pipeline(stages = stages)
    pipelineModel = pipeline.fit(train_transaction)
    df = pipelineModel.transform(train_transaction)
    selectedCols = ['label', 'features'] + cols
    df = df.select(selectedCols)
    df = df.drop("TransactionID")
    print("TransactionID column has been dropped!")
    df.printSchema()
    
    print("Hot encoding complete!")
    train_transaction.unpersist()
    
    #***************************************************#
    
    
    #### DATASET SPLITTING ####
    
    train, test = df.randomSplit([0.7, 0.3], seed=2021)
    print("Training Dataset Count: " + str(train.count()))
    print("Test Dataset Count: " + str(test.count()))
    
    if enableClassWeights:
        dataset_size = float(train.select("isFraud").count())
        numPositives = train.select("isFraud").where("isFraud == 'yes'").count()
        per_ones = (float(numPositives)/float(dataset_size))*100
        numNegatives = float(dataset_size-numPositives)
        BalancingRatio = numNegatives/dataset_size
        print('The number of ones are: {}'.format(numPositives))
        print('Percentage of ones are: {}'.format(per_ones))
        print('BalancingRatio: {}'.format(BalancingRatio))

        train = train.withColumn("classWeights", when(train.isFraud == 'yes',BalancingRatio).otherwise(1-BalancingRatio))
        train.select("classWeights").where("isFraud == 'yes'").show(5)
        train.select("classWeights").where("isFraud == 'no'").show(5)

    #### MODEL TRAINING AND EXECUTION ####
    
    def classifier_executor(classifier, train, test):
        model = classifier.fit(train)
        predictions = model.transform(test)
        predictions.select('label', 'rawPrediction', 'prediction', 'probability').show(10)
        metrics_calc(predictions)
    
    def metrics_calc(predictions):
        evaluator = BinaryClassificationEvaluator()
        print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))        
    
        numSuccesses = predictions.where("""(prediction = 0 AND isFraud = 'no') OR (prediction = 1 AND (isFraud = 'yes'))""").count()
        numInspections = predictions.count()
    
        print('There were', numInspections, 'inspections and there were', numSuccesses, 'successful predictions')
        print('This is a', str((float(numSuccesses) / float(numInspections)) * 100) + '%', 'success rate')
    
        true_positive = predictions.filter((predictions.prediction==1) & (predictions.isFraud=='yes')).count()
        false_positive = predictions.filter((predictions.prediction==0) & (predictions.isFraud=='yes')).count()
        true_negative = predictions.filter((predictions.prediction==0) & (predictions.isFraud=='no')).count()        
        false_negative = predictions.filter((predictions.prediction==1) & (predictions.isFraud=='no')).count()
    
        print("True positive: " + str(true_positive)) 
        print("False positive: " + str(false_positive)) 
        print("True negative: " + str(true_negative)) 
        print("False negative: " + str(false_negative)) 
    
        sensitivity = true_positive/(true_positive+false_negative)
        fallout = false_positive/(false_positive+true_negative)
        specificity = true_negative/(true_negative+false_positive)
        miss_rate = false_negative/(false_negative+true_positive)
    
        print("Sensitivity: " + str(sensitivity))
        print("Fallout: " + str(fallout))
        print("Specificity: " + str(specificity))
        print("Miss_rate: " + str(miss_rate))

    ## LR
    if logReg:
        classifier = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
        classifier_executor(classifier, train, test)
        if enableClassWeights:
            classifier = LogisticRegression(featuresCol = 'features', labelCol = 'label', weightCol = 'classWeights', maxIter=10)
            classifier_executor(classifier, train, test)

    # DT
    if decTree:
        classifier = DecisionTreeClassifier(featuresCol = 'features', labelCol = 'label', maxDepth = 3)
        classifier_executor(classifier, train, test)

    spark.stop()
