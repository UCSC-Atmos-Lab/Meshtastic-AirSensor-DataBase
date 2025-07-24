import sys
import bson
import pymongo

client = pymongo.MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.5.5")
targetDataBase = client["learning-mongo"]
targetCollection = targetDataBase["TestCollection"]
fileToRead = sys.argv[1]
with open(fileToRead, "r") as file:
    content = file.readlines()
stripCounter = 0
for i in content:
    content[stripCounter] = i.strip()
    querryDict = {"Name":"test_file_hello_world","Line":content[stripCounter], "Line Number":stripCounter}
    targetCollection.insert_one(querryDict)
    stripCounter = stripCounter + 1
