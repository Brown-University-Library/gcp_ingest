import json
import os
from rq import Queue
from redis import Redis
import requests
from dotenv import load_dotenv

class ResponseError(RuntimeError):
    pass

def check_response(resp,message):
    if not resp.ok:
            raise ResponseError(f"{message} - Response not ok: {resp.status_code} - {resp.headers}")
    response = resp.json()["response"]
    if not response:
        raise ResponseError(f"{message} - No response")
    return response

def queue_job(queue_name, function_name, function_args=None, function_kwargs=None):
    function_args = function_args or []
    function_kwargs = function_kwargs or {}
    q = Queue(queue_name, connection=Redis())
    return q.enqueue_call(func=function_name, args=function_args, kwargs=function_kwargs, timeout=72000)

def queue_create_stream_job(pid, datastream_or_url=None, visibility="BDR_PUBLIC"):
    kwargs={'visibility': visibility}
    if datastream_or_url:
        kwargs['datastream_or_url'] = datastream_or_url
    return queue_job(queue_name='stream_objects', function_name='stream_objects.create', function_args=(pid,), function_kwargs=kwargs)

def get_top_level_items(api_url,collection):
    # Select every top level item from collection, up to 9999
    params = {
        "q":f"rel_is_member_of_collection_ssim:{collection}",
        "fq":"!rel_is_part_of_ssim:['' TO *]",
        "rows":9999
    }
    response = requests.get(api_url,params)
    if not response.ok:
        print(f"Response not ok: {response.status_code} - {response.headers}")
        return
    print(f'found {response.json()["response"]["numFound"]} items...')
    return response.json()["response"]["docs"]

def get_child_with_filename(api_url,pid,filename):
    resp = requests.get(api_url,params={
        "q":f'rel_is_part_of_ssim:{pid} mods_id_filename_ssim:{filename}'
    })
    try:
        response = check_response(resp,f"{pid}, {filename}")
    except ResponseError:
        return
    if response["numFound"] != 1:
        print(f'more than one matching child found for {pid} - {filename}: {[doc["pid"]+" - "+doc["mods_id_filename_ssim"] for doc in response["docs"]]}')
        return
    item = response["docs"][0]

    return item

def select_stream_from_item_pid(api_url,pid):
    resp = requests.get(api_url,params={
        "q":f"rel_is_derivation_of_ssim:{pid} object_type:stream"
    })
    response = check_response(resp,f"{pid} stream")
    if response['numFound'] != 1:
        print(f"more than one stream found for {pid}")
    return response['docs']

def add_stream_to_rels(pid, panoptoId):
    params = {
        'pid':pid,
        'rels': json.dumps({'panoptoId': panoptoId}),
        'permission_ids':json.dumps([os.environ['API_IDENTITY']]),
        'message': "gcp rels ext update",
        'agent_name':"gcp ingest"
    }
    r = requests.put(os.environ["API_URL"],data=params)
    if not r.ok:
        raise Exception(f'{r.status_code} - {r.text}')

def get_stream_id(pid,api_url):
    resp=requests.get(api_url+pid)
    item = resp.json()
    stream_obj = item['relations']['hasPart'][0]
    panopto_id = stream_obj.get('rel_panopto_id_ssi')
    return panopto_id

def main():
    load_dotenv()
    api_url = os.environ["SOLR_URL"]
    item_api = os.environ["API_URL"]
    collection = os.environ["COLLECTION_PID"]

    resp = requests.get(api_url,params={
        "q":f"rel_is_member_of_collection_ssim:{collection} object_type:video"
    })
    try:
        response = check_response(resp)
    except ResponseError:
        print("Error on main query")
        return
    print(f"found {response['numFound']} items")

    for doc in response['docs']:
        print(f"queueing job for {doc['pid']}")
        # For all videos in collection
        #   - Queue job for stream creation

    parents = get_top_level_items(api_url,collection)
    for parent in parents:
        pid = parent['pid']
        filename = parent['identifierFileName']
        matched_child = get_child_with_filename(api_url,pid,filename)
        panoptoId = get_stream_id(matched_child['pid'],item_api)
        add_stream_to_rels(pid,panoptoId)

if __name__ == "__main__":
    main()
