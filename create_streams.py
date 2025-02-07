import os
from rq import Queue
from redis import Redis
import requests
from dotenv import load_dotenv

def queue_job(queue_name, function_name, function_args=None, function_kwargs=None):
    function_args = function_args or []
    function_kwargs = function_kwargs or {}
    q = Queue(queue_name, connection=Redis())
    q.enqueue_call(func=function_name, args=function_args, kwargs=function_kwargs, timeout=72000)

def queue_create_stream_job(pid, datastream_or_url=None, visibility="BDR_PUBLIC"):
    kwargs={'visibility': visibility}
    if datastream_or_url:
        kwargs['datastream_or_url'] = datastream_or_url
    queue_job(queue_name='stream_objects', function_name='stream_objects.create', function_args=(pid,), function_kwargs=kwargs)

def main():
    load_dotenv()
    api_url = os.environ["SOLR_URL"]
    collection = os.environ["COLLECTION_PID"]
    params = {
        "q":f"rel_is_member_of_collection_ssim:{collection}",
        "fq":"!rel_is_part_of_ssim:['' TO *]",
        "rows":500
    }
    response = requests.get(api_url,params)
    if not response.ok:
        print(f"Response not ok: {response.status_code} - {response.headers}")
        return
    print(f'found {response.json()["response"]["numFound"]} items...')
    docs = response.json()["response"]["docs"]
    for doc in docs:
        filename = doc["mods_id_filename_ssim"][0]
        pid = doc["pid"]
        resp = requests.get(api_url,params={
            "q":f'rel_is_part_of_ssim:{pid} mods_id_filename_ssim:{filename}'
        })
        if not resp.ok:
            print(f"Response not ok for {pid} - {filename}: {resp.status_code} - {resp.headers}")
            continue
        response = resp.json()["response"]
        if not response:
            print(f"No response for {pid} - {filename}")
            continue
        if response["numFound"] != 1:
            print(f'more than one equal child found for {pid} - {filename}: {[doc["pid"]+" - "+doc["mods_id_filename_ssim"] for doc in response["docs"]]}')
            continue
        item = response["docs"][0]
        print(f"parent {pid} will be assigned stream link for item {item['pid']}")
        # Queue job for stream creation
        # Get stream from job? or maybe sleep & get from item?
        # edit parent rels with stream link

if __name__ == "__main__":
    main()
