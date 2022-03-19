import pandas as pd
import threading
from datetime import datetime, timedelta
import sqlalchemy
from sqlalchemy import exc
import numpy
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen

with open("dblink.txt", "r") as f:
    dblink = f.read()
    engine = sqlalchemy.create_engine(dblink)



def db():
    query0 = "CREATE TABLE IF NOT EXISTS searches(s_id SERIAL NOT NULL, searchlink TEXT NOT NULL UNIQUE, PRIMARY KEY(s_id));"
    query1 = "CREATE TABLE IF NOT EXISTS targets(dataid INT NOT NULL,s_id INT NOT NULL,link TEXT NOT NULL,val0 FLOAT NOT NULL,  PRIMARY KEY(dataid), CONSTRAINT s_id_fk foreign key(s_id) references searches(s_id));"
    query2 = "CREATE TABLE IF NOT EXISTS values(dataid INT NOT NULL,dt DATE NOT NULL, val_n FLOAT NOT NULL, PRIMARY KEY(dataid,dt),CONSTRAINT dataid_fk foreign key(dataid) references targets(dataid)); "
    q = (query0, query1, query2)
    for Q in q:
        sendquery(Q)


def sendquery(query: str):
    with engine.connect() as con:
        response = con.execute(query)
        return response
def sqla_to_pd_df(sqla):
    df0 = sqla.mappings().all()
    df=pd.DataFrame(df0)
    return df

def targets(KA_Link,s_id):
    req = Request(KA_Link, headers={"User-Agent": "Mozilla/5.0"})
    webpage = urlopen(req).read()
    websoup = BeautifulSoup(webpage, "html.parser")
    items = websoup.find_all("li", class_="ad-listitem lazyload-item")
    df = {"dataid": [],'s_id':[], "link": [], "val0": []}
    df1 = pd.DataFrame(df)

    for i in items:
        j = i.find("p", class_="aditem-main--middle--price")
        price = j.text[41:].split(" ")[0]
        try:
            price=float(price)
        except:
            continue
        did = i.find("article")
        df = {
            "dataid": [did.attrs["data-adid"]],
            "s_id":[s_id],
            "link": [did.attrs["data-href"]],
            "val0": [float(price)],
        }
        df2 = pd.DataFrame(df)
        df1 = pd.concat([df1, df2], ignore_index=True)
    df1.to_sql("targets", engine, if_exists="append", index=False)
    return

def add_search(searchlink):
    try:
        df={
            'searchlink':[searchlink]
        }
        df1=pd.DataFrame(df)
        df1.to_sql('searches',engine,if_exists='append',index=False)
        with engine.connect() as con:
            res = con.execute(f"select * from searches where searches.searchlink='{searchlink}'")
            s_id=res.one()[0]
            print(s_id)
        targets(searchlink,s_id)
    except sqlalchemy.exc.IntegrityError:
        print("Link already exists in the search table")
        return
def interval_scrape():
    targetdf=pd.read_sql_table('targets',engine)
    for i in range(len(targetdf['s_id'])):
        dataid=targetdf['dataid'][i]
        print(dataid)

#TODO find a sensitive method of scraping at time intervals
#add link 1. add link, search new targets, scrape initial info for the targets
#interval scrapes:1.get all links from the targets folder, scrape the information.
if __name__ == "__main__":


   interval_scrape()