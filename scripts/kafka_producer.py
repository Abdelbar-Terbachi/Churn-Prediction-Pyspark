from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, PCA
from pyspark.sql.functions import col
from pyspark.ml.feature import Imputer
import json
from imblearn.over_sampling import RandomOverSampler
from collections import Counter

# Initialize Spark session
spark = SparkSession.builder.appName('customer_churn_producer').getOrCreate()

# Load 'new_customers.csv'
df = spark.read.csv('new_customers.csv', header=True, inferSchema=True)

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'customer_churn'

# Create Kafka Producer
producer_conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(producer_conf)


# SmartEncoder class for preprocessing
class SmartEncoder():
    use_onehot = False
    use_std_scaler = False
    use_pca = False

    def __init__(self, df1, pred, use_oversampler, use_onehot, use_std_scaler, use_pca):
        print("Constructor of Class SmartEncoder")

        self.df1 = df1
        self.pred = pred
        self.use_oversampler = use_oversampler
        self.use_onehot = use_onehot
        self.use_std_scaler = use_std_scaler
        self.pca = use_pca

    @staticmethod
    def checkNulls(df):
        null_cols = {col: df.filter(df[col].isNull()).count() for col in df.columns}
        return null_cols

    @staticmethod
    def checkDuplicates(df):
        if df.count() > df.dropDuplicates().count():
            raise ValueError('Data has duplicates')

    def handlingMissingVal(self):
        # Drop cols when the whole row is null
        df = self.df1.na.drop(how="all")
        # Drop duplicates
        df = df.dropDuplicates()

        # Treating missing values
        cols_to_drop = [x for x in df.columns if df.filter(df[x].isNull()).count() > 0]
        if len(cols_to_drop) != 0:
            dff = df.select(cols_to_drop)
            continuousCols = [item[0] for item in dff.dtypes if item[1] != 'string']
            imputer = Imputer(inputCols=continuousCols, outputCols=continuousCols).setStrategy("mean")
            model = imputer.fit(df)
            imputed_data = model.transform(df)
        else:
            imputed_data = df

        return imputed_data

    def indexString(self):
        imputed = self.handlingMissingVal()
        if self.use_oversampler:
            imputed = self.oversample(imputed)

        # Index the string to numeric
        self.stringCols = [item[0] for item in imputed.dtypes if item[1] == 'string']
        if len(self.stringCols) != 0:
            outputs = [y + "_encoded" for y in self.stringCols]
            stringIndexer = StringIndexer(inputCols=self.stringCols, outputCols=outputs)
            model = stringIndexer.fit(imputed)
            result = model.transform(imputed)

            encoded = result.drop(*self.stringCols)

            if self.use_onehot:
                # Converting categorical attributes into a binary vector
                encoder = OneHotEncoder(dropLast=False, inputCols=outputs,
                                        outputCols=[x + "_vec" for x in self.stringCols])
                encoded2 = encoder.fit(encoded).transform(encoded)

                encoded = encoded2.drop(*outputs)

            encoded = encoded.withColumnRenamed(self.pred + '_encoded', self.pred)
            encoded = encoded.withColumnRenamed(self.pred + '_vec', self.pred)
        else:
            encoded = imputed

        return encoded

    def oversample(self, df):
        dfp = df.toPandas()
        oversample = RandomOverSampler()
        data, y = oversample.fit_resample(dfp.loc[:, dfp.columns != self.pred], dfp[self.pred])
        data[self.pred] = y
        df_sample = spark.createDataFrame(data)
        return df_sample

    def dataAssembler(self, drops):
        # VectorAssembler - transform features into a feature vector column
        encoded = self.indexString()
        encoded = encoded.drop(*drops)
        assembler = VectorAssembler(inputCols=encoded.drop(self.pred).columns, outputCol='features')
        df_assembled = assembler.transform(encoded)

        if self.use_std_scaler:
            # Standardize the dataframe to ensure that all the variables are around the same scale
            scale = StandardScaler(inputCol='features', outputCol='standardized')
            df_scale = scale.fit(df_assembled)
            df_assembled = df_scale.transform(df_assembled)

        if self.pca:
            pca = PCA(k=3, inputCol=df_assembled.columns[-1])
            pca.setOutputCol("pca_features")
            model = pca.fit(df_assembled)
            df_assembled = model.transform(df_assembled)

        return df_assembled


# Function to preprocess a single row using SmartEncoder
def preprocess_row(row):
    # Convert the row to a Pandas DataFrame
    df_row = spark.createDataFrame([row])

    # Apply preprocessing using SmartEncoder
    obj = SmartEncoder(df_row, 'Churn', True, True, True, False)
    df_processed = obj.dataAssembler('')

    # Extract features from the processed DataFrame
    assembler = VectorAssembler(inputCols=df_processed.drop('Churn').columns, outputCol='features')
    df_assembled = assembler.transform(df_processed)
    features = df_assembled.select('features').collect()[0]['features']

    return features


# Create Kafka Producer
producer_conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(producer_conf)

# Produce data to Kafka
for _, row in df.toPandas().iterrows():
    # Preprocess each row
    features = preprocess_row(row)

    # Convert features to JSON and send to Kafka
    message = json.dumps({"features": features.tolist()})
    producer.produce(topic_name, message)

# Close the producer
producer.flush()
