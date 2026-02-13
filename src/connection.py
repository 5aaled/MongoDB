import pandas as pd ,os
from pymongo import MongoClient

df =pd.read_csv("~/Documents/education/projets livarbles/5/healthcare_dataset.csv")

df.columns = ( df.columns.str.strip().str.lower().str.replace(" ", "_")
)
df["age"] = df["age"].astype(int)
df["billing_amount"] = df["billing_amount"].astype(float)
df["room_number"] = df["room_number"].astype(int)
df["date_of_admission"] = pd.to_datetime(df["date_of_admission"])
df["discharge_date"] = pd.to_datetime(df["discharge_date"])
df = df.drop_duplicates(subset=['name', 'gender', 'blood_type', 'medical_condition','date_of_admission', 'doctor', 'hospital', 'insurance_provider','billing_amount', 'room_number', 'admission_type', 'discharge_date','medication', 'test_results'])
invalid_billing = df[df["billing_amount"] < 0]
df.loc[df["billing_amount"] < 0, "billing_amount"] = 0
records = df.to_dict(orient="records")
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/medical_db")
client = MongoClient(mongo_uri)
db = client["medical_db"]
collection = db["patients"]
# collection.insert_many(records)
# print("Data successfully migrated to MongoDB!")
