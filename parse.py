import os
import sys
from pkgutil import simplegeneric
import json
import requests
import csv
from flask import Flask, render_template, request, send_from_directory, current_app
import webbrowser


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
total_field_number = 0;
my_json, current_fields, current_specific_fields = {}, [], []
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
    dic = dic[str(current_index)]
    if (dic.get("aggs")):
        dic = dic["aggs"];
    current_index += 1;

    while(dic.get(str(current_index))):
        dic = dic[str(current_index)]
        if(dic.get("aggs")):
            dic = dic["aggs"];
        current_index += 1;

    dic["by_top_hit"] = hits_append["by_top_hit"];

    return current_index - 1;

def filter_data(data, current_index, request):
    lst = [];

    def populate_list(bucket):



        for entry in bucket["by_top_hit"]["hits"]["hits"]:
            for key in request.keys():
                print(key, entry["_source"].get(key), request[key])
                if(request[key] == "All keys"):
                    lst.append(entry["_source"]);
                    break;
                elif(entry["_source"].get(key) == None):
                    break;
                elif(entry["_source"].get(key).lower() != request.get(key).lower()):
                    break;
            else:
                lst.append(entry["_source"]);

    def recurse(buckets, index):
        for bucket in buckets:
            if(index == current_index):
                populate_list(bucket);
            else:
                recurse(bucket[str(index + 1)]["buckets"], index + 1);



    buckets = data["aggregations"]["2"]["buckets"]
    recurse(buckets, 2)
    return lst;

def get_fields(json_obj):
    dic = json_obj["aggs"];
    current_index = 2;
    fields = [];
    while (dic.get(str(current_index))):
        dic = dic[str(current_index)];
        fields.append(dic["terms"]["field"]);
        if(dic.get("aggs")):
            dic = dic["aggs"]
        current_index += 1;
    return fields;

def create_csv(lst, filename = "csv/raw_data.csv"):
    print(len(lst))
    keys = lst[0].keys();
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader();
        for entry in lst:
            dic = {}
            for key in keys:
               if(entry.get(key)):
                try:
                    dic[key] = entry.get(key).encode('ascii', 'ignore').decode('ascii');
                except:
                    dic[key] = entry.get(key);
               else:
                dic[key] = 'NULL'
            dict_writer.writerow(dic);

    return filename;

@app.route('/')
def render_webpage():
    return render_template("WebApp.html")

@app.route('/download', methods = ["GET", "POST"])
def download():
    return send_from_directory(directory = 'csv', filename = 'raw_data.csv')

def get_data_fields(data, fields, current_index):

    def recurse(buckets, index, lst_of_fields = []):

        for bucket in buckets:
            if(index < current_index):
                dic = bucket[str(index + 1)]["buckets"]
                recurse(dic, index + 1, lst_of_fields);
            lst_of_fields[index - 2][bucket["key"]] = 0;


    lst_of_fields = [];
    for i in range(0, current_index - 1):
        lst_of_fields.append({});

    index = 2;

    buckets = data["aggregations"][str(index)]["buckets"]
    recurse(buckets, index, lst_of_fields)
    return lst_of_fields;

@app.route('/filter', methods = ['POST'])
def filter():
    request_dic = {};

    for i, field in enumerate(current_fields):
        request_dic[field] = request.form.get(str(i)).capitalize();
        print(request_dic[field])

    lst = filter_data(my_json, total_field_number + 1, request_dic)
    filename = create_csv(lst);
    return render_template("Download_Ready_Web_App.html")

@app.route('/python', methods = ['POST'])
def parse():
    message = request.form['message'];

    json_str = ''.join(message);
    strip_whitespace(json_str);
    json_obj = json.loads(json_str);

    fields = get_fields(json_obj);

    current_index = add_hits(json_obj)
    r = requests.post(url = URL, json = json_obj)
    data = r.json();

    data_fields = get_data_fields(data, fields, current_index);
    names = [];
    for i in range(0, len(fields)):
        names.append(str(i));
    global total_field_number, current_fields, current_specific_fields, my_json;

    total_field_number = len(fields);
    my_json = data
    current_fields = fields;


    return render_template("Scrollbars.html", num_of_fields = len(fields), data_fields = data_fields, names = names)


if __name__ == "__main__":
    webbrowser.open('http://localhost:3000', new=1)
    app.run(port = 3000, debug = True)

