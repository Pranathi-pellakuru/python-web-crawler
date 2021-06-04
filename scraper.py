import requests
from bs4 import BeautifulSoup as bs
import random,string
import os
from urllib.parse import urlparse
from database import add_to_database,update_data,connect_database
from cfg import database_connection,db_name,collection_name,parser


def crawl(x):
    records=connect_database(database_connection,db_name,collection_name)
    crawlstate=x["Is_Crawled"]
    if crawlstate==False:
        try: 
            source = requests.get(x["Link"],timeout=10) 
            htmlcode=bs(source.text,parser)
            fp=writeto_random_file(htmlcode)                               #creates a random file and writes the html code to it
            links=validlinks(htmlcode,x)                                   #lists all the valid links in the html code
            p=add_to_database(links,records,x)                             #adds documents of the valid links into the database
            update_data(x,records,source,fp)                               #updates the document of the crawled link
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
            htmlcode=bs(source.text)
            #to rewrite the contents in the randomfile that already exists without creating a new file
            fp=rewrite_the_file(x,htmlcode)
            links=validlinks(htmlcode,x)                         #lists all the valid links in the html code
            add_to_database(links,records,x)                     #adds the documents of new links
            update_data(x,records,source,fp)                     #update the document of just crawled data
        except requests.exceptions.HTTPError as httpErr: 
            pass
        except requests.exceptions.ConnectionError as connErr: 
            pass
        except requests.exceptions.Timeout as timeOutErr: 
            pass
        except requests.exceptions.RequestException as reqErr: 
            pass

def writeto_random_file(htmlcode):
    random_file=''.join(random.choices(string.ascii_uppercase+string.digits,k=10))       #creates the random file
    with open(f'{random_file}.html','w',encoding="utf-8") as f:
        f.write(htmlcode.prettify())                                                     #adds the html code to the random file
    f.close()                         
    fp=((os.path.dirname(__file__))+f"\\{random_file}.html")                             #path to the random file
    #filepath of the randomfile
    return fp
def rewrite_the_file(x,htmlcode):
    with open(x["File Path"],'r+') as fpp:                                            #rewrites the contents in the already existing file
        fpp.seek(0)
        fpp.truncate()
        fpp.write(htmlcode.prettify())
    fpp.close()
    return x["File Path"]
def validlinks(htmlcode,x):
    links=[]
    domain=urlparse(x["Link"]).netloc                                   #domain of the link that is being crawled
    sche=urlparse(x["Link"]).scheme                                     #scheme of the link that is being crawled
    for atag in htmlcode.find_all('a',href=True):                       #iterates through all the atags that contain href
        z=atag["href"]                                             

        if z.startswith("#"):
            continue
        elif z.startswith('tel:'):
            continue
        elif "http" in z:
            links.append(z)
        elif z.startswith('//'):
            links.append('https:'+z)
        elif z=='/':                                                        #links that don't take us to another page
            continue
        elif z.startswith('/'):                                                     #to deal with the relative links
            z=sche+'://'+domain+z
            links.append(z)
        elif z.startswith("javascript:"):
            continue
    return links                                                            #returns list of valid links


         
    
    

