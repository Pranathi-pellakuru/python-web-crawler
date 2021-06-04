from pymongo import MongoClient
import certifi
from datetime import timedelta,datetime 
from cfg import limit

def connect_database(database_connection,db_name,collection_name):
    #connecting to the mongodb server
    client=MongoClient(database_connection,tlsCAFile=certifi.where())
    #connecting to mongodb database
    db=client.get_database(db_name)
    #connecting to the collection links
    record=db[collection_name]
    return record

def add_rooturl(url,record):
    #adds the root url into the database
    #first document that will be inserted into the database
    link = {
            'Link': url,
            'Source_Link': '',
            'Is_Crawled': False,
            'Last_Crawl_Dt': '',
            'Response_Status': '',
            'Content_type': '',
            'Content_length':'',
            'File_path': '',
            'Created_at': datetime.now(),
           }
    record.insert_one(link)
def countdocs(record):
    #counts the number of documents in the record
    count=record.count_documents({})
    return count

def links_to_be_crawled(record):
    #lists the documents of the links that are yet to be crawled or that are crawled 24hrs ago
    presentlinks =list(record.find({"$or": [{"Is_Crawled": False},{"Last_Crawl_Dt":{ "$lt": datetime.now() - timedelta(days=1)}}]}))
    return presentlinks

def add_to_database(links,record,x):
    #checks if the link is already present in the database
    #if not it adds the link
    for i in links:
        link_in_database=list(record.find({'Link':i}))
        #to check if the url exists in the database if so skipping 
        #if not adding to the database
        if len(link_in_database)==0:
            insertdata  = {
                            'Link': i,
                            'Source_Link': x["Link"],
                            'Is_Crawled': False,
                            'Last_Crawl_Dt': '',
                            'Response_Status':'',
                            'Content_type': '',
                            'Content_length':'',
                            'File_path': "",
                            'Created_at': datetime.now()                       
                        }
            record.insert_one(insertdata)
        else:
            continue 



def update_data(x,record,source,fp):
    #updates the document of the link that's been crawled
    updatedata = {
                    'Is_Crawled': True,
                    'Last_Crawl_Dt': datetime.now(),
                    'Response_Status': source.status_code,
                    'Content_type': source.headers['Content-Type'],
                    'Content_length': source.headers['Content-Length'] if "Content-Length" in source.headers else '',
                    'File_path': fp,
                 }
    record.update_one({ "_id": x["_id"]}, { "$set": updatedata})
