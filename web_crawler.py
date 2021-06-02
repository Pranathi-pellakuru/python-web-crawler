import requests
import random,string
import time as newt
from bs4 import BeautifulSoup as bs
import concurrent.futures
from datetime import datetime,timedelta
import os
from pymongo import MongoClient
import urllib
import certifi

#connecting to the mongodb server

client=MongoClient("mongodb+srv://root:"+urllib.parse.quote('Pragya@123')+"@cluster0.zh2ni.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",tlsCAFile=certifi.where())

#connecting to mongodb database


db=client.get_database("Flinklin")

#connecting to the collection links

record=db.links
rooturl="https://flinkhub.com"
 #adding rooturl document into collection links 
link = {
            'Link': rooturl,
            'Source Link': '',
            'Is Crawled': False,
            'Last Crawl Dt': '',
            'Response Status': '',
            'Content type': '',
            'Content length':'',
            'File path': '',
            'Created at': datetime.now()
        }
record.insert_one(link)


#defining the cycle function
#it basically list all the links that are not crawled so far and the links that are crawled 24hrs ago
#and pass each link to the function crawl(x)


def cycle():
    linkspresent =list(record.find({"$or": [{"Is Crawled": False},{"Last Crawl Dt":{ "$lt": datetime.now() - timedelta(days=1)}}]}))
    if len(linkspresent)==0:
        return True
        #returns true when there are no links that should be crawled
    else:
        #multithreading using threadpoolexecutor with maximum of 5 threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures=[]
            try:
                for lnk in linkspresent:
                    futures.append(executor.submit(crawl,lnk))
                for future in concurrent.futures.as_completed(futures):
                    future.result()
            except Exception as error:
                print(error)
        newt.sleep(5)
        #time module has been aliased as newt
        #after every cycle the process sleeps for 5 secs
        return False
      
      
def crawl(x):
    crawlstate=x["Is Crawled"]
    if crawlstate==False:
        try: 
            source = requests.get(x["Link"],timeout=10) 
            source.raise_for_status()
            htmlcode=bs(source.text,"lxml")
            #creating a random file and storing the html code of the website into it
            random_file=''.join(random.choices(string.ascii_uppercase+string.digits,k=10))
            with open(f'htmlFiles/{random_file}.html','w',encoding="utf-8") as f:
                f.write(htmlcode.prettify())
            f.close()
            fp=((os.path.dirname(__file__))+f"\\htmlFiles\\{random_file}.html")    #filepath of the randomfile
            
            for atag in htmlcode.find_all('a',href=True):
                count=record.count_documents({})          
                #count the number of documents if the limit is not reached it crawl the website
                #else returns -1
                if count<50:
                    z=atag["href"]
                    if z=="/":
                        #this removes all the dummy links that takes us no where
                        continue
                    elif "http" in z:
                        rurl=z
                    elif "/" in z:
                        #to add domain to the relative links
                        r=x["Link"]+z
                        try:
                            res=request.get(r)
                            if(res):
                                rurl=r
                        except Exception as e:
                            continue
                    else:
                        continue
                    link_in_database=list(records.find({'Link':rurl}))
                    #to check if the url exists in the database if so skipping 
                    #if not adding to the database
                    if len(link_in_database)==0:
                        insertdata  = {
                                        'Link': rurl,
                                        'Source Link': x["Link"],
                                        'Is Crawled': False,
                                        'Last Crawl Dt': '',
                                        'Response Status':'',
                                        'Content type': '',
                                        'Content length':'',
                                        'File path': "",
                                        'Created at': datetime.now()
                                    }
                    #adding the new links into the database
                    #rechecking the count and adding the new data if limit is not execeeded 
                    #since we are using multithreading rechecking helps to minimize the cases where limit exceeds
                        count=record.count_documents({})
                        if count<50:
                            record.insert_one(insertdata)
                        else:
                            return -1
                    else:
                        continue
                     
                else:
                    return -1
            #updating the document of the link thats been crawled
            #adding all the details like last crawl date and file path
            updatedata = {
                            'Is Crawled': True,
                            'Last Crawl Dt': datetime.now(),
                            'Response Status': source.status_code,
                            'Content type': source.headers['Content-Type'],
                            'Content length': source.headers['Content-Length'] if "Content-Length" in source.headers else '',
                            'File path': fp,
                        }
            record.update_one({ "_id": x["_id"]}, { "$set": updatedata})
            #print("update data completed")
            return 0 
        #to handle the exceptions when requests.get is used               
        except requests.exceptions.HTTPError as httpErr: 
            pass
        except requests.exceptions.ConnectionError as connErr: 
            pass
        except requests.exceptions.Timeout as timeOutErr: 
            pass
        except requests.exceptions.RequestException as reqErr: 
            pass
    

    else:
        try: 
            source = requests.get(x["Link"],timeout=10) 
            source.raise_for_status()
            htmlcode=bs(source.text,"lxml")
            #to rewrite the contents in the randomfile after being crawled without creating a new file
            with open(x["File Path"],'r+') as fpp:
                fpp.seek(0)
                fpp.truncate()
                fpp.write(htmlcode.prettify())
            fpp.close()
            
            for atag in htmlcode.find_all('a',href=True):
                count=record.count_documents({})
                if count<50:
                    z=atag["href"]
                    if z=="/":
                        continue
                    elif "http" in z:
                        rurl=z
                    elif z[0]=="/":
                        rurl=x["Link"]+z
                    else:
                        continue
                    link_in_database=list(records.find({'Link':rurl}))
                    #to check if the url exists in the database if so skipping 
                    #if not adding to the database
                    if len(link_in_database)==0:
                        insertdata  = {
                                        'Link': rurl,
                                        'Source Link': x["Link"],
                                        'Is Crawled': False,
                                        'Last Crawl Dt': '',
                                        'Response Status':'',
                                        'Content type': '',
                                        'Content length':'',
                                        'File path': "",
                                        'Created at': datetime.now()
                                    }
                        record.insertone(insertdata)
                    else:
                        continue
                     
                else:
                    print("limit exceeded")
                    return -1
            updatedata = {
                            'Last Crawl Dt': datetime.now(),
                            'Response Status': source.status_code,
                            'Content length': source.headers['Content-Length'] if "Content-Length" in source.headers else ''
                        }
            record.update_one({ "_id": x["_id"]}, { "$set": updatedata})
            return 0
        except requests.exceptions.HTTPError as httpErr: 
            pass
        except requests.exceptions.ConnectionError as connErr: 
            pass
        except requests.exceptions.Timeout as timeOutErr: 
            pass
        except requests.exceptions.RequestException as reqErr: 
            pass
while True:
    count=record.count_documents({})
    if count<50:
        progress=cycle()
        #cycel function returnd true when there are no links that are to be crawled
        if progress:
            print("all links are crawled")
            break
        else:
            print("next cycle")

    else:
        print("limit exceeded")
        break
