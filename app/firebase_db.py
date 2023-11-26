from firebase_admin import db
import firebase_admin
from firebase_admin import credentials

cred = credentials.Certificate(
    "/media/dell/5AA456FFA456DD57/Workspace/hust/big-data/serviceAccountKey.json")
default_app = firebase_admin.initialize_app(cred, {
    'databaseURL': "https://big-data-7ecd3-default-rtdb.asia-southeast1.firebasedatabase.app/"
})

ref = db.reference("/emails")


def add_data(data):
    print("Saving data to firebase with message id: ", data["Message-ID"])
    ref.push(data)
