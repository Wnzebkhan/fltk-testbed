from time import time
from sklearn.datasets import make_classification
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV

import pandas as pd
from sklearn.preprocessing import LabelEncoder

# read the data and creat the dataset
df = pd.read_csv("bank-full.csv", delimiter=";")
encoded = df.apply(LabelEncoder().fit_transform)

x = encoded.iloc[:, :16]
y = encoded.iloc[:, 16]

# define the model
#model = RandomForestCalssifier(n_estimator = , n_jobs = )

#start =

#perform search
# model.fit()

#record current time
#end =

#report execution time
#result = end - start
#print()