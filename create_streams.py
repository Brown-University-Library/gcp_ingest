import argparse
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
        "q":f'rel_is_part_of_ssim:{pid} \
            mods_id_filename_ssim:{filename} \
            object_type:video'
    })
    try:
        response = check_response(resp,f"{pid}, {filename}")
    except ResponseError:
        return
    if response["numFound"] != 1:
        print(f'more than one matching child found for {pid} - {filename}: {str([doc["pid"]+" - "+str(doc["mods_id_filename_ssim"]) for doc in response["docs"]])}')
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
        'rels': json.dumps({'panopto_id': panoptoId}),
        'permission_ids':json.dumps([os.environ['API_IDENTITY']]),
        'message': "gcp rels ext update",
        'agent_name':"gcp ingest"
    }
    # TODO: add stream cmodel to rels... seems to need xml, see link:
    # https://github.com/Brown-University-Library/bdr_apis_project/blob/0f176eb800ca7c31b45822f291e69784d14153f7/items_app/metadata.py#L837
    r = requests.put(os.environ["API_URL"],data=params)
    if not r.ok:
        raise Exception(f'{r.status_code} - {r.text}')

def get_stream_id(pid,api_url):
    resp=requests.get(api_url+pid)
    item = resp.json()
    stream_obj = item['relations']['hasDerivation'][0]
    resp_s=requests.get(api_url+stream_obj['pid'])
    item_s = resp_s.json()
    panopto_id = item_s.get('rel_panopto_id_ssi')
    return panopto_id

def gcp_make_streams(api_url,collection):
    # create stream for all videos in collection
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
        queue_create_stream_job(doc['pid'])

def gcp_attach_streams_to_parents(api_url,collection,item_api):
    # for all parent items, attach stream id of name-matched item to parent item
    parents = get_top_level_items(api_url,collection)
    for parent in parents:
        pid = parent['pid']
        filename = parent['identifierFileName']
        matched_child = get_child_with_filename(api_url,pid,filename)
        panoptoId = get_stream_id(matched_child['pid'],item_api)
        add_stream_to_rels(pid,panoptoId)

def main():
    load_dotenv()
    api_url = os.environ["SOLR_URL"]
    item_api = os.environ["API_URL"]
    collection = os.environ["COLLECTION_PID"]

    parser = argparse.ArgumentParser(
        description="makes streams and adds stream to parent for GCP"
    )

    parser.add_argument("-q","--queue-stream-jobs",
        action="store_true",
        help="queue stream jobs for GCP collection",
        dest='queue'
    )
    parser.add_argument("-a","--add-stream-to-parents",
        action="store_true",
        help="add stream IDs to parents in GCP collection",
        dest='add'
    )

    args = parser.parse_args()

    if args.queue and args.add:
        print("can't make streams and add to parent at once, please allow time for stream generation")
        parser.print_help()
        return
    if args.queue:
        print("queueing jobs for full gcp collection")
        gcp_make_streams(api_url,collection)
        return
    if args.add:
        print("attaching streams to parents for full gcp collection")
        gcp_attach_streams_to_parents(api_url,collection,item_api)
        return
    parser.print_help()

if __name__ == "__main__":
    main()
