# %% Imports
from flask import Flask, jsonify, render_template, request, url_for, flash, redirect
import subprocess
import sqlite3
import sys
import os
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
# from flask_sqlalchemy import SQLAlchemy

# %%  Statics

# SQLite URI compatible
WIN = sys.platform.startswith('win')
if WIN:
    prefix = 'sqlite:///'
else:
    prefix = 'sqlite:////'

DATABASE = os.path.dirname(__file__)+os.sep+'pyTestWellData.db'

app = Flask(__name__)

# %% Part 2 -- Get endpoint 

def get_db_connection(): #Establishes connection for eqch query, returns connection
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def query_db(conn, query): #performs query to connection and closes connection, returns row object
    cur = conn.execute(query)
    rv = cur.fetchall()
    conn.close()
    return rv

@app.route("/")
def index():
    return render_template("base.html")

@app.route('/getWellData/', methods=(["GET", "POST"]))
def getWellData():
    if request.method=="GET":
        return render_template('getWellData.html')
    elif request.method=="POST":
        # print(url_for("/updateWellData", APIkey=request.form['Well API']))
        return redirect(f"/updateWellData/{request.form['Well API']}")

@app.route('/updateWellData/<APIKey>', methods=(["GET", "POST"]))
def get_data_byAPI(APIKey):
    if request.method=="GET":
        conn = get_db_connection()
        query = f'SELECT * FROM api_well_data where API == "{APIKey}"'
        posts = query_db(conn, query)
        return render_template('updateWellData.html', posts=posts)
    else:
        return

@app.route('/getWellsbyLocation/', methods=(["GET", "POST"]))
def getWellsbyLocation():
    if request.method=="GET":
        return render_template('getWellsbyLocation.html')
    elif request.method=="POST":
        # print("HERE")
        # print(request.form)
        # print(type(request.form['coordinate list']))
        # a=list(request.form['coordinate list'])
        # print(a)
        # print(url_for("/updateWellData", APIkey=request.form['Well API']))
        return redirect(f"/updateWellList/{request.form['coordinate list']}")

@app.route('/updateWellList/<coordList>', methods=(["GET", "POST"]))
def get_wells_byList(coordList):
    if request.method=="GET":
        conn = get_db_connection()
        query = f'SELECT API, Latitude, Longitude, CRS FROM api_well_data'
        posts = query_db(conn, query)
        #create polygon based on list of lat longs
        polygon = Polygon(list(eval(coordList)))
        #filter based on polygon
        tempList=[]
        for post in posts:
            coord=Point(float(post['Latitude']),float(post['Longitude']))
            # print(polygon.contains(coord))
            if polygon.contains(coord):
                tempList.append(post)
        return render_template('updateWellList.html', posts=tempList)
    else:
        return



if __name__ == '__main__':
    app.run()
    
#part 1. test API: 30-015-25404

#part 2. [(32.81,-104.19),(32.66,-104.32),(32.54,-104.24),(32.50,-104.03),(32.73,-104.01),(32.79,-103.91),(32.84,-104.05),(32.81,-104.19)]