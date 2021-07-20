from fastapi import FastAPI, Request
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from datetime import datetime, timedelta


app = FastAPI()
import os, time, ssl, redis
import requests





@app.get("/toplastday")
def toplastday():
    result = []
    r = redis.Redis(host="redis", port=6379, db=0)
    keys = r.scan_iter("*-01*")
    print(keys)
    for key in keys:
        print({key:r.get(key)})
        result.append({key:r.get(key)})

    return result



@app.get("/lasthourhashtags/{last_hours}")
def toplastday(last_hours: int):
    result = []
    r = redis.Redis(host="redis", port=6379, db=0)
    last_hour_date_time = datetime.now() - timedelta(hours = last_hours)
    hour = last_hour_date_time.hour
    print(hour)
    if int(hour) < 10:
        hour = "0"+str(hour)
    keys = r.scan_iter("*-"+str(hour))
    cnt = 0
    for key in keys:
        cnt = cnt + 1
    return {"Number of last hour hashtags": cnt}
