from database import countdocs,links_to_be_crawled,connect_database,add_rooturl
from scraper import crawl
import time
import concurrent.futures
from cfg import database_connection,db_name,collection_name,rooturl,max_threads,limit
def cycle(records):
    linkspresent =links_to_be_crawled(records)
    if len(linkspresent)==0:
        return True
        #returns true when there are no links that should be crawled
    else:
        #multithreading using threadpoolexecutor with maximum of 5 threads
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
                executor.map(crawl,linkspresent)     
                 
            time.sleep(5)
        
        #after every cycle the process sleeps for 5 secs
        except Exception as e:
            print(e)
        return False
if __name__=="__main__":
    records=connect_database(database_connection,db_name,collection_name)
    #connects to the database
    add_rooturl(rooturl,records)
    while True:
        count=countdocs(records)
        if count<limit:
            progress=cycle(records)
            #cycle returns true when there are no links that are to be crawled
            if progress:
                print("all links are crawled")
                break
            else:
                print("next cycle")

        else:
            print("limit exceeded")
            break
