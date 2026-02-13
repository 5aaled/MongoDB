from pymongo import MongoClient
from connection import df
print(df.shape)
# check negative Values
invalid_billing = df[df["billing_amount"] < 0]
print(invalid_billing.shape)    # the result should be ZEro


client = MongoClient("mongodb://localhost:27017/")
db = client["medical_db"]
patients = db["patients"]

# insert one ligne
patient = {"name": "Alice Brown",  "age": 34,  "gender": "Female", "blood_type": "A+","medical_condition": "Hypertension", "billing_amount": 1800}
#patients.insert_one(patient)

patients_list = [{ "name": "Bob Smith","age": 50,"gender": "Male", "medical_condition": "Diabetes",  "billing_amount": 2200 }, {"name": "Clara Jones","age": 29,"gender": "Female", "medical_condition": "Asthma",  "billing_amount": 900 }]
#patients.insert_many(patients_list)

# 2 : READ #########################

# for patient in patients.find({"gender": "Female"}):
   # print(patient["name"], patient["age"])

# N  of rows
df_NB = len(df)
DB_NB = patients.count_documents({})

# Select one column / field
df_select_name = df["name"].head()
DB_select_name = patients.find_one({}, {"_id": 0, "name": 1})

# Select MULTIPLE columns
df_multi_columns = df[["name", "age", "medical_condition"]].head()
DB_multi_columns=patients.find_one({},{"_id": 0, "name": 1, "age": 1, "medical_condition": 1})


# WHERE condition (EQUAL)
df_where = df[df["medical_condition"] == "Diabetes"].head()
DB_where = patients.find_one({"medical_condition": "Diabetes"})
# WHERE with numeric condition (>)
df_condition = df[df["age"] > 60].head()
db_condition = patients.find_one({"age": {"$gt": 60}})

# WHERE with TWO conditions (AND)
df_condition2 = df[(df["gender"] == "Female") & (df["age"] > 40)].head()
db_condition2 = patients.find_one({ "gender": "Female",  "age": {"$gt": 40}})


# SELECT + WHERE
df_condition3 = df.loc[df["medical_condition"] == "Diabetes", ["name", "billing_amount"]]
db_condition3 =  patients.find_one({"medical_condition": "Diabetes"},{"_id": 0, "name": 1, "billing_amount": 1})


# limit result
df_limit = df.head(3)
db_limit = list(patients.find().limit(3))

# sort
df_sort = df.sort_values(by="billing_amount",ascending=False)
db_sort = list(patients.find().sort("billing_amount", -1).limit(5))


# distinct
df_dist = df["medical_condition"].unique()
db_dist =patients.distinct("medical_condition")

# COUNT with condition
df_cond = len(df[df['gender'] == 'Male'])
db_cond = patients.count_documents({"gender": "Male"})


#################### Group by #################################################

df_count = df.groupby("medical_condition").count()["name"]
pipeline = [ { "$group": { "_id": "$medical_condition", "count": {"$sum": 1} } }]
db_count = list(patients.aggregate(pipeline))


#GROUP BY (SUM)   # total biling amount
df_sum = df.groupby("medical_condition")["billing_amount"].sum()
pipeline2 = [ { "$group": { "_id": "$medical_condition","total_billing": {"$sum": "$billing_amount"} } }]

db_sum = list(patients.aggregate(pipeline2))
## AVG
df_avg = df.groupby("medical_condition")["age"].mean()
pipeline3 = [{ "$group": {"_id": "$medical_condition", "avg_age": {"$avg": "$age"}  } }]
db_avg = list(patients.aggregate(pipeline3))

# GROUP BY with MULTIPLE aggregations
df_multi_agg =df.groupby("medical_condition").agg( patient_count=("name", "count"), total_billing=("billing_amount", "sum"),avg_age=("age", "mean"))
pipeline4 = [{"$group": {"_id": "$medical_condition","patient_count": {"$sum": 1},"total_billing": {"$sum": "$billing_amount"}, "avg_age": {"$avg": "$age"} }}]

db_multi_agg =list(patients.aggregate(pipeline4))


### GROUP BY + WHERE
df_cond11 = df[df["gender"] == "Female"].groupby("medical_condition")["billing_amount"].sum().reset_index()
pipeline5 = [
    {"$match": {"gender": "Female"}},
    {"$group": {"_id": "$medical_condition", "total_billing": {"$sum": "$billing_amount"} } }]

db_cond11 =  list(patients.aggregate(pipeline5))

### GROUP BY + HAVING
df_cond22 = df.groupby("medical_condition")["billing_amount"].sum()
df_filter = df_cond22[df_cond22 >  2.138010e+08]
pipeline6 = [{ "$group": {"_id": "$medical_condition", "total_billing": {"$sum": "$billing_amount"} } } ,
           { "$match": {"total_billing": {"$gt": 10000}} }]
db_cond22 = list(patients.aggregate(pipeline6))


#####    group by  + Sort
pipeline7 = [ {"$group": {"_id": "$medical_condition","total_billing": {"$sum": "$billing_amount"} }},
             {"$sort": {"total_billing": -1}},
             {"$limit": 5}]
db_sort11 =   list(patients.aggregate(pipeline7))






