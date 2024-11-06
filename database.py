from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient

DATABASE_URL_1 = 'mssql+pyodbc://@3B3LLQ2/ADF?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
DATABASE_URL_2 = 'mssql+pyodbc://@3B3LLQ2/db1?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
DATABASE_URL_3 = 'mssql+pyodbc://@3B3LLQ2/db2?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'


engine_1 = create_engine(DATABASE_URL_1)
engine_2 = create_engine(DATABASE_URL_2)
engine_3 = create_engine(DATABASE_URL_3)

SessionLocal_1 = sessionmaker(autocommit=False, autoflush=False, bind=engine_1)
SessionLocal_2 = sessionmaker(autocommit=False, autoflush=False, bind=engine_2)
SessionLocal_3 = sessionmaker(autocommit=False, autoflush=False, bind=engine_3)

def get_db_1():
    db = SessionLocal_1()
    try:
        yield db
    finally:
        db.close()

def get_db_2():
    db = SessionLocal_2()
    try:
        yield db
    finally:
        db.close()

def get_db_3():
    db = SessionLocal_3()
    try:
        yield db
    finally:
        db.close()


MONGO_URL = 'mongodb+srv://commonuser:commonuser123@cluster0.czzze.mongodb.net/Logs?retryWrites=true&w=majority&appName=Cluster0'
mongo_client = MongoClient(MONGO_URL)
mongo_db = mongo_client.get_database('Logs')
mongo_collection = mongo_db.get_collection('akarsh')

def insert_to_mongo(data):
    result = mongo_collection.insert_one(data)
    print(f'One post: {result.inserted_id}')
    return result.inserted_id



def setup_databases(db_url):
    engine = create_engine(db_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    def get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    
    return SessionLocal


def setup_mongo_collection(MONGO_URL):
    # MONGO_URL = 'mongodb+srv://commonuser:commonuser123@cluster0.czzze.mongodb.net/Logs?retryWrites=true&w=majority&appName=Cluster0'
    mongo_client = MongoClient(MONGO_URL)
    mongo_db = mongo_client.get_database('Logs')
    mongo_collection = mongo_db.get_collection(MONGO_URL)
    
    def insert_to_mongo(data):
        result = mongo_collection.insert_one(data)
        print(f'One post: {result.inserted_id}')
        return result.inserted_id
    
    return insert_to_mongo
