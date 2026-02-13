# Medical Data Migration to MongoDB

## 1. Project Context

This project was carried out as part of a Big Data and NoSQL mission.
A client provided a medical dataset in CSV format and requested a scalable
solution to store and manage the data using MongoDB.

The objective of this project is to migrate the dataset from a CSV file into
a MongoDB database, perform basic CRUD operations, and document deployment
options for scalability.

---

## 2. Dataset Description

The dataset contains medical information about patients, including:

- Name
- Age
- Gender
- Blood Type
- Medical Condition
- Date of Admission
- Doctor
- Hospital
- Insurance Provider
- Billing Amount
- Room Number
- Admission Type
- Discharge Date
- Medication
- Test Results

Each row in the CSV corresponds to one patient and is stored as a MongoDB document.

---

## 3. Technical Stack

- Python 3
- Pandas
- MongoDB
- PyMongo
- MongoDB Compass (for visualization)
- GitHub (version control)

---

## 4. Data Migration Process

The migration process is implemented in Python and follows these steps:

1. Load the CSV file using pandas.
2. Normalize column names (lowercase, no spaces).
3. Convert data types (dates, integers, floats).
4. Remove duplicate records.
5. Convert the DataFrame into a list of JSON-like documents.
6. Insert the documents into a MongoDB collection (`patients`).

The database used is `medical_db` and the collection is `patients`.

---

## 5. MongoDB Authentication & Roles

To secure access to the database, two MongoDB users were created:

- **medical_app_user**
  - Role: `readWrite` on `medical_db`
  - Used by the Python application for data migration and CRUD operations.

- **medical_read_user**
  - Role: `read` on `medical_db`
  - Intended for read-only access and analytics.

Users were created using the MongoDB shell (`mongosh`).

---

## 6. CRUD Operations

Basic CRUD (Create, Read, Update, Delete) operations were tested using Python.
Examples include:

- Reading patients by gender or age
- Sorting patients by billing amount
- Aggregating data using MongoDB aggregation pipelines (group by medical condition)

These operations validate that the migration was successful and that the data
can be queried efficiently.

---

## 7. How to Run the Project

1. Install MongoDB and ensure it is running locally.
2. Create the MongoDB users using `mongosh`.
3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
## 8. Dockerized Deployment

The project can be executed using Docker and Docker Compose.
Two containers are used:
- MongoDB container with persistent storage using Docker volumes
- Python application container for data migration

Docker volumes ensure that MongoDB data persists across container restarts.
Environment variables are used to configure database connections securely.