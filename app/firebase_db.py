import boto3
from firebase_admin import db
import firebase_admin
from firebase_admin import credentials
from dotenv import load_dotenv
import os

load_dotenv()

cred = credentials.Certificate(
    "/media/dell/5AA456FFA456DD57/Workspace/hust/big-data/app/serviceAccountKey.json")
default_app = firebase_admin.initialize_app(cred, {
    'databaseURL': os.getenv('DATABASE_URL')
})

ref = db.reference("/laptops")


def add_data(data):
    ref.push(data)


def add_normal_data(data):
    print("Add normal data", data)
    ref.child("normal").push(data)


def add_business_data(data):
    print("Add business data", data)
    ref.child("business").push(data)


def add_office_data(data):
    print("Add office data", data)
    ref.child("office").push(data)


def add_gaming_data(data):
    print("Add gaming data", data)
    ref.child("gaming").push(data)
