# importing Require libraries
import streamlit as st
from apify_client import ApifyClient
import pandas as pd
from PIL import Image
import time
import math
import re

st.title(":blue[Search Engine Results Page Crawler]")

# Declaring Global Variable
global countries, url_dict

# Country codes
countries = ["in", "us", "gb", "au", "de", "lk", "bd", "pk", "af", "al", "dz", "as", "ad", "ao", "ai", "aq",
             "ag", "ar", "am", "aw", "at", "az", "bs", "bh", "bb", "by", "be", "bz", "bj", "bm", "bt", "bo",
             "ba", "bw", "bv", "br", "io", "bn", "bg", "bf", "bi", "kh", "cm", "ca", "cv", "ky", "cf", "td",
             "cl", "cn", "cx", "cc", "co", "km", "cg", "cd", "ck", "cr", "ci", "hr", "cu", "cy", "cz", "dk",
             "dj", "dm", "do", "ec", "eg", "sv", "gq", "er", "ee", "et", "fk", "fo", "fj", "fi", "fr", "gf",
             "pf", "tf", "ga", "gm", "ge", "gh", "gi", "gr", "gl", "gd", "gp", "gu", "gt", "gn", "gw", "gy",
             "ht", "hm", "va", "hn", "hk", "hu", "is", "id", "ir", "iq", "ie", "il", "it", "jm", "jp", "jo",
             "kz", "ke", "ki", "kp", "kr", "kw", "kg", "la", "lv", "lb", "ls", "lr", "ly", "li", "lt", "lu",
             "mo", "mk", "mg", "mw", "my", "mv", "ml", "mt", "mh", "mq", "mr", "mu", "yt", "mx", "fm", "md",
             "mc", "mn", "ms", "ma", "mz", "mm", "na", "nr", "np", "nl", "an", "nc", "nz", "ni", "ne", "ng",
             "nu", "nf", "mp", "no", "om", "pw", "ps", "pa", "pg", "py", "pe", "ph", "pn", "pl", "pt", "pr",
             "qa", "re", "ro", "ru", "rw", "sh", "kn", "lc", "pm", "vc", "ws", "sm", "st", "sa", "sn", "cs",
             "sc", "sl", "sg", "sk", "si", "sb", "so", "za", "gs", "es", "sd", "sr", "sj", "sz", "se", "ch",
             "sy", "tw", "tj", "tz", "th", "tl", "tg", "tk", "to", "tt", "tn", "tr", "tm", "tc", "tv", "ug",
             "ua", "ae", "um", "uy", "uz", "vu", "ve", "vn", "vg", "vi", "wf", "eh", "ye", "zm", "zw"]

url_dict = {"title": [], "url": [], "displayedUrl": [], "country": []}


def apify_actor(txt, number):
    with st.spinner('Wait for it...'):
        time.sleep(5)
        for country in countries:
            if len(url_dict["url"]) < number:
                # Initialize the ApifyClient with API token
                client = ApifyClient("apify_api_dd99KwRhTHxYy0n4gjQKEjqsMW6ufa0t6mVp")

                # Prepare the actor input
                run_input = {
                    "queries": txt,
                    "maxPagesPerQuery": math.ceil(number / 100),
                    "resultsPerPage": 100,
                    "countryCode": country,
                    "customDataFunction": """async ({ input, $, request, response, html }) => {
              return {
                pageTitle: $('title').text(),
              };
            };""",
                }

                # Run the actor and wait for it to finish
                run = client.actor("apify/google-search-scraper").call(run_input=run_input)
                page_count = 0

                # Fetch and print actor results from the run's dataset (if there are any)
                for item in client.dataset(run["defaultDatasetId"]).iterate_items():
                    if 'url' in item.keys():
                        page_count += 1
                        # print(item["organicResults"])
                        len_OR = len(item["organicResults"])
                        for j in range(len_OR):
                            url_dict["title"].append(item["organicResults"][j]['title'])
                            url_dict["url"].append(item["organicResults"][j]['url'])
                            url_dict["displayedUrl"].append(item["organicResults"][j]['displayedUrl'])
                            url_dict["country"].append(country)
                time.sleep(5)
                print(len(url_dict["url"]))
                print("Number of Page scraped: ", page_count)
                print("country: ", country)

        return url_dict


# Item returns json format result. and it returns following keys with results
@st.cache_data
def convert_df(url_dict):
    # converting Dict To Dataframe
    df = pd.DataFrame(url_dict)

    # To know shape of dataset
    st.success("Shape of Your Dataset is " + str(df.shape), icon="✅")

    # To display head of dataset
    st.header(":blue[Head of Dataset]")
    Head_DF = df.head()
    st.dataframe(Head_DF)

    # TO display tail of dataset
    st.header(":blue[Tail of Dataset]")
    Tail_DF = df.tail()
    st.dataframe(Tail_DF)

    # Converting Dataframe to CSV
    csv_file = df.to_csv().encode('utf-8')
    urlCSV_file = df["url"].to_csv().encode('utf-8')
    return df, csv_file, urlCSV_file

def clear_form():
    time.sleep(2)
    st.success("Check Your Downloads File", icon="✅")
    st.balloons()
    time.sleep(2)
    st.session_state["text"] = ""
    st.session_state["num"] = 100.00

def main():
    # placeholder = st.empty()
    #status = False
    df = None
    csv = None
    with st.form("my_form"):
        txt = st.text_input('Enter the Topic for crawling from Google Search Engine Result Page', key="text")
        number = st.number_input('Number of Data Need to Scrape', key="num", min_value=100)
        number = int(number)
        # text = txt

        if st.form_submit_button("Submit"):
            global url_dict
            url_dict = apify_actor(txt, number)

    if len(url_dict['url']) != 0:
        df, csv, urlCSV = convert_df(url_dict)

    name = re.sub(r"[^a-zA-Z0-9 ]", " ", txt)
    name = name.replace(" ", "_")

    #st.write(st.session_state)

    if df is not None:
        st.download_button(
            label="Download data as CSV File",
            data=csv,
            file_name=name + "_SERP.csv",
            mime='text/csv',
            on_click=clear_form,
        )

        st.download_button(
            label="Download as jsonl",
            data="\n".join([str(d) for d in url_dict]),
            file_name=name + "_SERP.jsonl",
            mime="application/jsonl",
            on_click = clear_form,
        )

        st.download_button(
            label="Download data as URL CSV File",
            data=urlCSV,
            file_name=name + "_SERP.csv",
            mime='text/csv',
            on_click=clear_form,
        )
        urlJSON = {"url": []}
        urlJSON["url"].append(url_dict["url"])

        st.download_button(
            label="Download as URL jsonl",
            data="\n".join([str(d) for d in urlJSON]),
            file_name=name + "_SERP.jsonl",
            mime="application/jsonl",
            on_click=clear_form,
        )

    # if status:
    #     time.sleep(2)
    #     # st.balloons()
    #     placeholder.empty()



main()




