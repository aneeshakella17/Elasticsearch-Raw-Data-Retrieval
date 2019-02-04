import os
import sys
from pkgutil import simplegeneric
import json
import requests
import csv
from flask import Flask, render_template, request, send_from_directory, current_app
import webbrowser

#TODO:
#STILL NEED TO ENSURE IT WORKS FOR ALL INPUT
app = Flask(__name__);

URL = "http://172.27.255.228:9200/version_string_sda/_search?scroll=1m";
SIZE = 10000;
my_json, current_fields = {}, []


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


#Remove whitespace from strings for standardization purposes
def strip_whitespace(json_data):
    for key, value in get_items(json_data):
        if hasattr(value, 'strip'): # json string
            json_data[key] = value.strip()
        else:
            strip_whitespace(value)


#Appendage added to original Kibana request to retrieve raw data. This piece should be added to the greatest aggregation number found
#within the data. For instance, if there are 3 subdivisions (subtech, theater, and amec), there will be a subdictionary
#within the data response called "4". The equation is thus (1 + # of subdivisions). The function searcehs for this greatest aggregation number,
#finds the dictionary associated with the number, and then adds the appendage.

def add_hits(json_obj):

    hits_append = {"by_top_hit": {
        "top_hits": {
            "size": SIZE
        }
    }
    }


    current_index = 2;
    dic = json_obj["aggs"][str(current_index)];

    if (dic.get("aggs")):
        dic = dic["aggs"];


    while(dic.get(str(current_index + 1))):
        dic = dic[str(current_index + 1)]
        if(dic.get("aggs")):
            dic = dic["aggs"];

        current_index += 1;

    dic["by_top_hit"] = hits_append["by_top_hit"];

    return current_index;



#There are two functions listed here. After we send the request modified by add_hits, we get the response. Now, this
#response needs to be analyzed to collect the raw data. Ultimately, this can be thought of having to collect the leaf nodes at the bottom of the tree.
#To do this, we utilize the recursive for-loop strategy as found in recurse. Once we get to the leaf node, we utilize populate_list
#to gather the individual data_entries
def filter_data(data, current_index, request):

    lst = [];


    def populate_list(bucket):
        entry_count = 0;
        for entry in bucket["by_top_hit"]["hits"]["hits"]:
            count = len(request.keys());
            for key in request.keys():
                if(request[key] == "ALL KEYS"):
                    count -= 1;
                elif(entry['_source'].get(key) == None):
                    break;
                elif(entry['_source'][key].lower() == request[key].lower()):
                    count -= 1;

            if(count == 0):
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


#Gather the specific subdivisions (theater, subtech, ...)
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

#Generate the output CSV file
def create_csv(lst, filename = "csv/raw_data.csv"):


    with open(filename, 'w') as output_file:
        if (len(lst) > 0):

            keys = lst[0].keys();
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader();

            for entry in lst:
                dic = {}

                for key in keys:
                    if (entry.get(key)):

                        try:
                            dic[key] = entry.get(key).encode('ascii', 'ignore').decode('ascii');
                        except:
                            dic[key] = entry.get(key);


                    else:
                        dic[key] = 'NULL'

                dict_writer.writerow(dic);
        else:
            pass;

    return filename;

@app.route('/')
def render_webpage():
    return render_template("WebApp.html")

@app.route('/download', methods = ["GET", "POST"])
def download():
    return send_from_directory(directory = 'csv', filename = 'raw_data.csv')


#Analyzes the response to figure out which data the user wants to analyze more closely. For instance, within subtech,
#the user can choose to analyze the different types of subtechs (assurance, security, ...), theaters (AMER, EPAC)
def get_data_fields(data, current_index):

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


#After the scrollbar section has been complete, the data is parsed for the selected sections.
@app.route('/filter', methods = ['POST'])
def filter():
    request_dic = {};

    for i, field in enumerate(current_fields):
        request_dic[field] = request.form.get(str(i));

    lst = filter_data(my_json, len(current_fields) + 1, request_dic);
    print("LST", len(lst))
    create_csv(lst);
    return render_template("Download_Ready_Web_App.html")


#Request the initial message from the client, gather the data fields, and then return the scrollbars so the user
#can decide how to filter the data )
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


    data_fields = get_data_fields(data, current_index);
    names = [];
    for i in range(0, len(fields)):
        names.append(str(i));

    global current_fields, my_json;

    my_json = data
    current_fields = fields;


    return render_template("Scrollbars.html", num_of_fields = len(fields), data_fields = data_fields, names = names)


if __name__ == "__main__":
    webbrowser.open('http://localhost:3000', new=1)
    app.run(port = 3000, debug = True)

