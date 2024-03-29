import requests
import json
from flask import Flask, send_file, render_template, request, send_from_directory
import csv
from datetime import date, datetime
import os
import re

app = Flask(__name__);

IP = "172.27.255.228"
URL = "http://{0}:9200/_search?scroll=1m".format(IP)

SIZE = 1000;
my_json, current_fields = {}, [];


def get_items(obj):
    while False:  # no items, a scalar object
        yield None


def _(obj):
    return obj.iteritems()  # json object


def _(obj):
    return enumerate(obj)


# Remove whitespace from strings for standardization purposes
def strip_whitespace(json_data):
    for key, value in get_items(json_data):
        if hasattr(value, 'strip'):  # json string
            json_data[key] = value.strip()
        else:
            strip_whitespace(value)


# Appendage added to original Kibana request to retrieve raw data. This piece should be added to the greatest aggregation number found
# within the data. For instance, if there are 3 subdivisions (subtech, theater, and amec), there will be a subdictionary
# within the data response called "4". The equation is thus (1 + # of subdivisions). The function searcehs for this greatest aggregation number,
# finds the dictionary associated with the number, and then adds the appendage.

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

    while (dic.get(str(current_index + 1))):
        dic = dic[str(current_index + 1)]
        if (dic.get("aggs")):
            dic = dic["aggs"];

        current_index += 1;

    dic["by_top_hit"] = hits_append["by_top_hit"];

    return current_index;

def compare_two_dates(date1, date2):
    date1 = date1.split('-');
    date2 = date2.split('-');
    a = date(int(date1[0]), int(date1[1]), int(date1[2]));
    b = date(int(date2[0]), int(date2[1]), int(date2[2]));
    return (a - b).days;

# There are two functions listed here. After we send the request modified by add_hits, we get the response. Now, this
# response needs to be analyzed to collect the raw data. Ultimately, this can be thought of having to collect the leaf nodes at the bottom of the tree.
# To do this, we utilize the recursive for-loop strategy as found in recurse. Once we get to the leaf node, we utilize populate_list
# to gather the individual data_entries
def filter_data(data, current_index, request):
    lst = [];
    visited = {};

    def populate_list(bucket):

        for entry in bucket["by_top_hit"]["hits"]["hits"]:
            count = len(request.keys());
            for key in request.keys():
                if (key == "report_date" and request[key] != "ALL KEYS"):
                    try:
                        days = compare_two_dates(entry['_source'][key][0:10], request[key])
                    except:
                        try:
                            t_index = entry['_source'][key].index('T')
                            report_date = entry['_source'][key][0:t_index]
                            days = compare_two_dates(report_date, request[key]);
                        except:
                            report_date = entry['_source']['time'][0:10];
                            days = compare_two_dates(report_date, request[key]);


                    if (days >= 0 and days <= interval):
                        count -= 1;
                    else:
                        break;

                elif (request[key] == "ALL KEYS"):
                    count -= 1;
                elif (entry['_source'].get(key) == None):
                    break;
                elif (entry['_source'][key].lower() == request[key].lower()):
                    count -= 1;
                elif (re.match(request[key].lower(), entry['_source'][key].lower()) != None):
                    count -= 1;

            if (count == 0 and visited.get(entry["_source"]["sr_id"]) == None and entry["_index"] == "version_string_sda"):
                visited[entry["_source"]["sr_id"]] = True;
                lst.append(entry["_source"]);

    def recurse(buckets, index):
        for bucket in buckets:
            if (index == current_index):
                if (type(bucket) is str or type(bucket) is unicode):
                    populate_list(buckets[bucket]);
                else:
                    populate_list(bucket);
            else:
                recurse(bucket[str(index + 1)]["buckets"], index + 1);

    buckets = data["aggregations"]["2"]["buckets"]
    recurse(buckets, 2)
    return lst;


# Gather the specific subdivisions (theater, subtech, ...)
def get_fields(json_obj):
    dic = json_obj["aggs"];
    current_index = 2;
    fields = [];

    while (dic.get(str(current_index))):

        dic = dic[str(current_index)];

        if (dic.get("terms") != None):
            fields.append(dic["terms"]["field"]);
        elif (dic.get("filters")):
            for key in dic["filters"]["filters"]:
                colon_index = key.index(':')
                if (colon_index == -1):
                    break;
                else:
                    fields.append(key[0:colon_index]);
                    break;

        elif (dic.get("date_histogram") or dic.get("date_range")):
            fields.append("report_date");

        if (dic.get("aggs")):
            dic = dic["aggs"]

        current_index += 1;

    return fields;


# Generate the output CSV file
def create_csv(lst, fname="raw_data.csv"):
    with open(fname, 'w+') as output_file:
        if (len(lst) > 0):

            keys = ["SR_SW_Version", "SR_SoftwareVersion", "script_name", "report_date",  "Component0", "Product0", "issues", "title", "feature", "DDTS", "subtech", "Projects", "company_short", "workgroup", "company", "DDTS0", "Products",	"Components", "sr_severity", "sw_version", "theater", "contract", "tech", "time", "sr_id", "_id", "Project0", "product_family"]
                                                                                                                                                                                                                          
            csv_writer = csv.DictWriter(output_file, fieldnames=keys);
            csv_writer.writeheader();

            for row, entry in enumerate(lst):
                dic = {}

                for column, key in enumerate(keys):
                    if (key == "report_date" and entry.get(key) == None and entry.get("time") != None):
                        dic["report_date"] = entry.get("time")[0:10]
                    elif (key == "report_date" or key == "time") and 'T' in entry.get(key):
                        index = entry.get(key).index('T')
                        if(key == "report_date"):
                            dic[key] = entry.get(key)[0:index]
                        elif(key == "time"):
                            period = entry.get(key).index('.');
                            dic[key] = entry.get(key)[index + 1:period]
                    else:
                        if entry.get(key):
                            try:
                                dic[key] = entry.get(key).encode('ascii', 'ignore').decode('ascii');
                            except:
                                dic[key] = entry.get(key);
                        else:
                            dic[key] = 'NULL'

                if dic.get("report_date") == None:
                    continue;

                csv_writer.writerow(dic);
        else:
            pass;
        output_file.close();

    return fname;


@app.route('/')
def render_webpage():
    return render_template("WebApp.html")


@app.route('/download', methods=["GET", "POST"])
def download():
    return send_file(filename, mimetype='text/csv', attachment_filename=filename, as_attachment=True);


# Analyzes the response to figure out which data the user wants to analyze more closely. For instance, within subtech,
# the user can choose to analyze the different types of subtechs (assurance, security, ...), theaters (AMER, EPAC)
def get_data_fields(data, current_index):

    def recurse(buckets, index, lst_of_fields):
        for bucket in buckets:
            if (index < current_index):
                dic = bucket[str(index + 1)]["buckets"]
                recurse(dic, index + 1, lst_of_fields);

            if (type(bucket) is str or type(bucket) is unicode):
                colon_index = bucket.index(':');
                bucket = bucket[(colon_index + 1)::];
                if (bucket[0] == '\'' or bucket[0] == '"'):
                    bucket = bucket[1:-1];
                lst_of_fields[index - 2][bucket] = 0
            else:
                if (bucket.get("key_as_string")):
                    t_index = bucket["key_as_string"].index('T')
                    report_date = bucket["key_as_string"][0:t_index];
                    lst_of_fields[index - 2][report_date] = 0;
                elif (current_fields[index - 2] == "report_date"):
                    lst_of_fields[index - 2][bucket["key"][0:10]] = 0;
                else:
                    lst_of_fields[index - 2][bucket["key"]] = 0;

    lst_of_fields = [];
    for i in range(0, current_index - 1):
        lst_of_fields.append({});
    index = 2;

    buckets = data["aggregations"][str(index)]["buckets"]
    recurse(buckets, index, lst_of_fields)
    return lst_of_fields;




# After the scrollbar section has been complete, the data is parsed for the selected sections.
@app.route('/filter', methods=['POST']) 
def filter():
    request_dic = {};

    for i, field in enumerate(current_fields):
        request_dic[field] = request.form.get(str(i));

    lst = filter_data(my_json, len(current_fields) + 1, request_dic);
    global filename;

    for file in os.listdir('.'):
        if file.endswith('.csv'):
            os.remove(file);

    filename = create_csv(lst, str(datetime.now()) + "raw_output.csv");
    return render_template("Download_Ready_Web_App.html")

# Request the initial message from the client, gather the data fields, and then return the scrollbars so the user
# can decide how to filter the data )
@app.route('/python', methods=['POST'])
def parse():
    message = request.form['message'];
    json_str = ''.join(message);
    strip_whitespace(json_str);
    json_obj = json.loads(json_str);

    fields = get_fields(json_obj);
    current_index = add_hits(json_obj)

    r = requests.post(url=URL, json=json_obj)
    data = r.json()


    global current_fields, my_json

    my_json = data
    current_fields = fields;

    data_fields = get_data_fields(data, current_index);

    sorted_data_fields = [];

    for field_section in data_fields:
        sorted_data_fields.append(sorted(field_section.keys()))

    names = [];
    for i in range(0, len(fields)):
        names.append(str(i));


    global interval;
    interval = 30;
    if("report_date" in fields):
        report_index = fields.index("report_date")
        if(len(sorted_data_fields[report_index]) > 1):
            interval = abs(compare_two_dates(sorted_data_fields[report_index][0], sorted_data_fields[report_index][1]));
	
    return render_template("Scrollbars.html", num_of_fields=len(fields), data_fields=sorted_data_fields, names=names)

@app.after_request
def add_header(r):
    """
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate, public, max_age=0"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    return r



if __name__ == "__main__":
    app.run(host='0.0.0.0', port=3000, debug=True)
