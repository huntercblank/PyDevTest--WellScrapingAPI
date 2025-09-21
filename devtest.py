# %% Imports
import aiohttp
import asyncio
import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup as bs


# %% Statics
URL= "https://wwwapps.emnrd.nm.gov/OCD/OCDPermitting/Data/WellDetails.aspx?api="

dataDict={
    "Operator":{"id":"well_details","name":"Operator"},
    "Status":{"id":"well_details","name":"Status"},
    "Well Type":{"id":"well_details","name":"WellType"},
    "Work Type":{"id":"well_details","name":"WorkType"},
    "Directional Status":{"id":"well_details","name":"DirectionalStatus"},
    "Multi-Lateral":{"id":"well_details","name":"MultiLateral"},
    "Mineral Owner":{"id":"well_details","name":"MineralOwner"},
    "Surface Owner":{"id":"well_details","name":"SurfaceOwner"},
    "Surface Location":{"id":"well_details","name":"Location"},
    "GL Elevation":{"id":"well_details","name":"GLElevation"},
    "KB Elevation":{"id":"well_details","name":"KB Elevation"},
    "DF Elevation":{"id":"well_details","name":"DFElevation"},
    "Single/Multiple Completion":{"id":"well_details","name":"Completions"},
    "Potash Waiver":{"id":"well_details","name":"PotashWaiver"},
    "Spud Date":{"id":"well_dates","name":"SpudDate"},
    "Last Inspection":{"id":"well_dates","name":"LastInspectionDate"},
    "TVD":{"id":"Depths","name":"TrueVerticalDepth"},
    "Latitude":{"id":"well_details","name":"Coordinates","splitLoc":1},
    "Longitude":{"id":"well_details","name":"Coordinates","splitLoc":2},
    "CRS":{"id":"well_details","name":"Coordinates","splitLoc":3},
    }

renameMap={
    "WellType":"Well Type",
    "WorkType":"Work Type",
    "DirectionalStatus":"Directional Status",
    "MultiLateral":"Multi-Lateral",
    "MineralOwner":"Mineral Owner",
    "SurfaceOwner":"Surface Owner",
    "Location":"Surface Location",
    "GLElevation":"GL Elevation",
    "KBElevation":"KB Elevation",
    "DFElevation":"DF Elevation",
    "Completions":"Single/Multiple Completion",
    "PotashWaiver":"Potash Waiver",
    "SpudDate":"Spud Date",
    "LastInspectionDate":"Last Inspection",
    "TrueVerticalDepth":"TVD",
    }

colOrder=["Operator",
"Status",
"Well Type",
"Work Type",
"Directional Status",
"Multi-Lateral",
"Mineral Owner",
"Surface Owner",
"Surface Location",
"GL Elevation",
"KB Elevation",
"DF Elevation",
"Single/Multiple Completion",
"Potash Waiver",
"Spud Date",
"Last Inspection",
"TVD",
"API",
"Latitude",
"Longitude",
"CRS",
]


wellList=["Operator", "Status", "WellType", "WorkType", "DirectionalStatus", "MultiLateral",
"MineralOwner", "SurfaceOwner", "Location", "GLElevation", "KBElevation", "DFElevation", 
"PotashWaiver", "Coordinates", "Completions"]
depthsList=["TrueVerticalDepth"]
eventsList=["SpudDate","LastInspectionDate"]

# %% Scrape Functions - PART 1 - sequential version

def scrapeWellByAPI(api):
    #get page and break into results sections based on ids
    df={}
    df['API']=[api]
    page = requests.get(URL+api)
    soup = bs(page.content, "html.parser") #type: bs4.BeautifulSoup
    results_well = soup.find(id="well_details") #type: bs4.element.Tag
    results_depth = soup.find(id="Depths")
    results_events = soup.find(id="well_dates")
    
    #iterate through each results and grab vals based on lists
    for span in results_well.find_all('span'):
        name = span['id'].split("lbl")[-1]
        text = span.text
        if name in wellList:
            if name=="Coordinates":
                textSplit=text.split(" ")
                df["Latitude"]=[textSplit[0].split(",")[0]]
                df["Longitude"]=[textSplit[0].split(",")[1]]
                df["CRS"]=[textSplit[1]]
            else:
                df[name]=[text]
    for span in results_depth.find_all('span'):
        name = span['id'].split("lbl")[-1]
        text = span.text
        if name in depthsList:
            df[name]=[text]
    for span in results_events.find_all('span'):
        name = span['id'].split("lbl")[-1]
        text = span.text
        if name in eventsList:
            df[name]=[text]
    return pd.DataFrame(df)


def main():
    
    #Load api list csv and convert to list
    apiDF=pd.read_csv(r"C:/Users/blankhx/Documents/Just Business/apis_pythondev_test.csv")
    apiList=apiDF['api'].to_list()
    
    tempDF=pd.DataFrame()
    #iterate list and join to temp df
    for api in apiList:
        singleDF = scrapeWellByAPI(api)
        tempDF=pd.concat([tempDF,singleDF])
    
    
    tempDF=tempDF.rename(renameMap)
    
# %% Scrape Functions - PART 1 - asynchronous version
        
sem = asyncio.Semaphore(3) #not sure on exact rate limit, but 3 sems works, 5 failed


#async version
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()
    
async def scrapeWellByAPI(api, session):
    #get page and break into results sections based on ids
    
    # page = requests.get(URL+api)
    df={}
    df['API']=[api]
    url=URL+api
    async with sem:
        html = await fetch(session, url)
    soup = bs(html, "html.parser") #type: bs4.BeautifulSoup
    results_well = soup.find(id="well_details") #type: bs4.element.Tag
    results_depth = soup.find(id="Depths")
    results_events = soup.find(id="well_dates")
    
    #iterate through each results and grab vals based on lists
    for span in results_well.find_all('span'):
        name = span['id'].split("lbl")[-1]
        text = span.text
        if name in wellList:
            if name=="Coordinates":
                textSplit=text.split(" ")
                df["Latitude"]=[textSplit[0].split(",")[0]]
                df["Longitude"]=[textSplit[0].split(",")[1]]
                df["CRS"]=[textSplit[1]]
            else:
                df[name]=[text]
    for span in results_depth.find_all('span'):
        name = span['id'].split("lbl")[-1]
        text = span.text
        if name in depthsList:
            df[name]=[text]
    for span in results_events.find_all('span'):
        name = span['id'].split("lbl")[-1]
        text = span.text
        if name in eventsList:
            df[name]=[text]
    return pd.DataFrame(df)

async def main():

    #Load apiDF
    apiDF=pd.read_csv(r"C:/Users/blankhx/Documents/Just Business/apis_pythondev_test.csv")
    apiList=apiDF['api'].to_list()
    #Setup tasks and results
    async with aiohttp.ClientSession() as session:
        tasks = [scrapeWellByAPI(api, session) for api in apiList]
        results = await asyncio.gather(*tasks)
        tempDF=pd.DataFrame()
        for df in results:
            tempDF=pd.concat([tempDF,df])
        tempDF=tempDF.rename(renameMap, axis=1)
    #force dtyping for table
    #create sqlite db with table for data
    conn = sqlite3.connect("pyTestWellData.db")
    c=conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS api_well_data (
        "Operator" text,
        "Status" text,
        "Well Type" text,
        "Work Type" text,
        "Directional Status" text,
        "Multi-Lateral" text,
        "Mineral Owner" text,
        "Surface Owner" text,
        "Surface Location" text,
        "GL Elevation" text,
        "KB Elevation" text,
        "DF Elevation" text,
        "Single/Multiple Completion" text,
        "Potash Waiver" text,
        "Spud Date" text,
        "Last Inspection" text,
        "TVD" text,
        "API" text,
        "Latitude" real,
        "Longitude" real,
        "CRS" text
        )""")
    conn.commit()
    #commit tempDF to sqlitedb
    tempDF.to_sql('api_well_data', conn, if_exists='append', index=False)
    conn.execute('SELECT * from api_well_data').fetchall()
    return tempDF


# %% - Point to Main
if __name__ == '__main__':
    await main() # run aysnc version







