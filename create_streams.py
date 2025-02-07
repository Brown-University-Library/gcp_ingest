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
    response = requests.get(api_url,params)
    if response.ok:
        docs = response.content
        print(docs)

if __name__ == "__main__":
    load_dotenv()
    api_url = os.environ["SOLR_URL"]
    collection = os.environ["COLLECTION_PID"]
    params = {
        "q":f"rel_is_member_of_collection_ssim:{collection}+object_type:video",
        "fl":"pid,rel_is_part_of_ssim",
        "rows":500
    }
    main()