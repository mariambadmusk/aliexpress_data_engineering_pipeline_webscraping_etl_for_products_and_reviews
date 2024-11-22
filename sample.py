
from bs4 import BeautifulSoup
import re
import json

def send_request(page, SearchText):
    # Send request to the server
    # Get the response
    # return response
    
    data = {"pageVersion":"7ece9c0cc9cf2052db74f0d1b26b7033",
    "target":"root",
    "data":{
        "isFromCategory":"y",
        "categoryUrlParams":"{\"q\":\"Towel\",\"s\":\"qp_nw\",\"osf\":\"categoryNagivateOld\",\"sg_search_params\":\"\",\"guide_trace\":\"abb6ed62-6e3f-4767-af71-647ca4a69b31\",\"scene_id\":\"30630\",\"searchBizScene\":\"openSearch\",\"recog_lang\":\"en\",\"bizScene\":\"categoryNagivateOld\",\"guideModule\":\"unknown\",\"postCatIds\":\"15,13\",\"scene\":\"category_navigate\"}",
        "g":"y",
        "origin":"y"},
        
        "page":page,
        "SearchText":SearchText,
        "eventName":"onChange",
        "dependency":[]
        }

    return data