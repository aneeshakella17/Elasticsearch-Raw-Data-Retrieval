import os
import sys
from pkgutil import simplegeneric
import json
import requests
import csv
from flask import Flask, render_template, request, send_from_directory, current_app


#TODO:
# 1."BUILD FRONTEND"
#  -> Figure out automation
#  -> Create file that will automatically download with button click

# 1.ENSURE IT WORKS WITH ALL INPUTS
# 2."ENTER FILTRATION ABILITIES"
# 3."ALLOW FOR MORE OR LESS FIELDS"

app = Flask(__name__);

URL = "http://172.27.255.228:9200/version_string_sda/_search?scroll=1m";
SIZE = 100;
@simplegeneric
def get_items(obj):
    while False: # no items, a scalar object
        yield None

@get_items.register(dict)
def _(obj):
    return obj.iteritems() # json object

@get_items.register(list)
def _(obj):
    return enumerate(obj)

def strip_whitespace(json_data):
    for key, value in get_items(json_data):
        if hasattr(value, 'strip'): # json string
            json_data[key] = value.strip()
        else:
            strip_whitespace(value)

def add_hits(json_obj):
    hits_append = {"by_top_hit": {
        "top_hits": {
            "size": SIZE
        }
    }
    }



    dic = json_obj["aggs"];
    current_index = 2;

    if(dic.get(str(current_index))):
        dic = dic[str(current_index)]
        if(dic.get("aggs")):
            dic = dic["aggs"];
            dic["by_top_hit"] = hits_append["by_top_hit"];
        current_index += 1;


    return current_index - 1;

def filter_data(data, current_index):
    lst = [];
    for category in data["aggregations"][str(current_index)]["buckets"]:
        for entry in category["by_top_hit"]["hits"]["hits"]:
            lst.append(entry["_source"])
    return lst;

def create_csv(lst, filename = "csv/raw_data.csv"):
    keys = lst[0].keys();
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader();
        for entry in lst:
            dic = {}
            for key in keys:
               if(entry.get(key)):
                dic[key] = entry.get(key);
               else:
                dic[key] = "NULL"
        dict_writer.writerow(dic);

    return filename;

@app.route('/')
def render_webpage():
    return render_template("WebApp.html")

@app.route('/download', methods = ["GET", "POST"])
def download():
    return send_from_directory(directory = 'csv', filename = 'raw_data.csv')



@app.route('/python', methods = ['POST'])
def parse():
    message = request.form['message'];
    json_str = ''.join(message);
    strip_whitespace(json_str);
    json_obj = json.loads(json_str);
    current_index = add_hits(json_obj)
    r = requests.post(url = URL, json = json_obj);
    data = r.json();
    lst = filter_data(data, current_index);
    filename = create_csv(lst);
    return render_template("Download_Ready_Web_App.html")


if __name__ == "__main__":
    app.run(port = 3000, debug = True)
