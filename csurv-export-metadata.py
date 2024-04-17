import argparse
import json
import logging
import requests
import time
import concurrent.futures
import io
import csv
import copy
from datetime import datetime, timezone
from urllib3.exceptions import InsecureRequestWarning

#
# Script to search messages and export their metadata to a CSV file.
#
#  - The ES query is read from a JSON file. 
#  - For ES 7.x, the query must contain a "bool" query inside the "query" section so a "doc_type" filter can be added by the script. 
#  - All metadata fields specified in "_source" "includes" list are saved.
#  - Only top-level metadata fields can be read and saved. 
#  - "size" and "sort" are set by the script and should not be set in the JSON query file.
#  - This is written for ES 5.x and ES 7.x.
#
# Usage: 
#   Save message metadata from multiple message indices.
#
#   python3 csurv-export-metadata.py mykg__message_types_*
#

#
# Set the following ES config. Use https for TLS. Set the es_server to an ES data node in the cluster.
#
es_server           = 'http://localhost:9200'
es_user             = 'elastic'
es_password         = ''

#
# Constants
#
# NUM_SLICES can be increased or decreased to reduce the search parallelism.
# BATCH_SIZE can be increased or decreased depending on the number and size of fields returned.
# SYSTEM_FIELDS to be saved can be changed as needed. Use [] for no system fields.
# DOC_TYPE can be changed for the appropriate document type search.
#
NUM_SLICES                  = 10
BATCH_SIZE                  = 10000
SYSTEM_FIELDS               = ['_id']
DOC_TYPE                    = 'message'

QUERY_HEADERS               = {'Content-Type': 'application/json'}
ES_KEEP_ALIVE               = '1m'
STATUS_INTERVAL             = 20

JSON_DOC_TYPE_FILTER        = '{"term":{"doc_type":"' + DOC_TYPE + '"}}'
JSON_TRACK_TOTAL_HITS       = '{"track_total_hits":true}'
JSON_SLICE                  = '{"slice":{"id":-1,"max":' + str(NUM_SLICES) + '}}'  
JSON_SIZE                   = '{"size":' + str(BATCH_SIZE) + '}'
JSON_SORT                   = '{"sort":["_doc"]}'
JSON_SCROLL_BODY            = '{"scroll":"' + ES_KEEP_ALIVE + '","scroll_id":""}'
OUTPUT_FILE                 = 'output.csv'

# Supported ES versions.
VERSION_ES5 = '5'
VERSION_ES7 = '7'

# Check search response for errors.
def check_search_response(response):
    if response.status_code == requests.codes.ok:
        return True
    elif response.status_code == requests.codes.not_found:
        return False
    else:
        logger.error(f'ES error:{response.json()}')
        response.raise_for_status()

# Get the initial batch of messages for a slice. 
def get_message_metadata_search(slice_id, index_pattern, query_body):
    
    if es_version == VERSION_ES5:
        url = es_server + '/' + index_pattern + '/' + DOC_TYPE + '/' + '_search' + '?scroll=' + ES_KEEP_ALIVE
    else:
        url = es_server + '/' + index_pattern + '/' + '_search' + '?scroll=' + ES_KEEP_ALIVE
        
    # Update the slice_id.       
    query_body['slice']['id'] = slice_id
    
    logger.debug('ES query URL: %s', url)
    data = json.dumps(query_body)
    logger.debug('ES query body: %s', data)

    response = requests.get(
        url,
        headers=QUERY_HEADERS,
        auth=(es_user, es_password),
        data=data,
        verify=False
    )
    check_search_response(response)

    return response.json()

# Get a subsequent batch of messages with the scroll_id. 
def get_message_metadata_scroll(scroll_id, query_body):
  
    url = es_server + "/" + "_search/scroll"
    
    # Update the scroll_id.
    query_body['scroll_id'] = scroll_id
    
    logger.debug('ES query URL: %s', url)
    data = json.dumps(query_body)
    logger.debug('ES query body: %s', data)

    response = requests.get(
        url,
        headers=QUERY_HEADERS,
        auth=(es_user, es_password),
        data=data,
        verify=False
    )
    check_search_response(response)

    return response.json()
           
# Get the ES version and check if it is supported.
def check_es_version():
    url = es_server + "/" 
       
    logger.debug('Getting ES version: %s', url)

    response = requests.get(
        url,
        auth=(es_user, es_password),
        verify=False
    )
    
    check_search_response(response)
    
    global es_version
    
    result = response.json()
    version_number = result['version']['number']
    es_version = version_number.split('.')[0]  
   
    if es_version is None and not (es_version == VERSION_ES5 or es_version == VERSION_ES7):
        raise Exception(f'Unsupported ES version: {version_number}.')
          
# Search for messages in one slice.   
def process_slice(slice_id, index_pattern, csv_writer, query_body, scroll_body, source_fields):
    last_status = time.time()
    total_msgs = 0
    stats_count = 0
    
    logger.info('Slice[%d] Starting search...', slice_id)
    
    results = get_message_metadata_search(slice_id, index_pattern, query_body)
    
    if es_version == VERSION_ES5:
        total_hits = results['hits']['total']
    else:
        total_hits = results['hits']['total']['value']
    
    logger.info('Slice[%d] Total hits: %d', slice_id, total_hits)
    
    hits = results['hits']['hits']
      
    while total_hits > 0 and len(hits) > 0:
        # Save a batch of message metadata.
        for hit in hits:
            values = list()
            
            if len(SYSTEM_FIELDS) > 0:
                for system_field in SYSTEM_FIELDS:
                    values.append(hit.get(system_field,''))
                
            if len(source_fields) > 0:
                source = hit['_source']           
                for source_field in source_fields:         
                    values.append(source.get(source_field,''))
                    
            logger.debug('values: %s', values)
            csv_writer.writerow(values)
            total_msgs += 1
            stats_count += 1
    
        scroll_id = results['_scroll_id']
                
        cur_time = time.time()
        if cur_time - last_status >= STATUS_INTERVAL:
            exec_time = cur_time - last_status
            docs_per_sec = stats_count / exec_time if stats_count > 0 else 0
            logger.info('Slice[%d] Processed: %d - %s, docs/sec: %.0f', 
                slice_id, total_msgs, '{:.0%} complete'.format(total_msgs / total_hits), docs_per_sec)
            
            last_status = cur_time
            stats_count = 0
            
        results = get_message_metadata_scroll(scroll_id, scroll_body)
        hits = results['hits']['hits']
    
    logger.info('Slice[%d] Completed search, total hits: %d, processed: %d', slice_id, total_hits, total_msgs)
    
    return total_hits

# Save message metadata to a CSV file.
def save_message_metadata_to_csv(index_pattern, query_body, scroll_body, source_fields):

    total_msgs = 0
        
    logger.info('Searching messages with %d slices...', NUM_SLICES)
    
    start_exec = time.time()
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as output:
        csv_writer = csv.writer(output, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        header_fields = SYSTEM_FIELDS.copy()
        header_fields.extend(source_fields)
        csv_writer.writerow(header_fields)
        logger.debug('header_fields: %s', header_fields)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_SLICES) as executor:
            futures = []
            for id in range(NUM_SLICES):
                futures.append(executor.submit(process_slice, slice_id=id, index_pattern=index_pattern, csv_writer=csv_writer, query_body=copy.deepcopy(query_body), scroll_body=copy.deepcopy(scroll_body), source_fields=source_fields))
            
            for future in concurrent.futures.as_completed(futures):
                total_msgs += future.result()              
            
    end_exec = time.time()    
    exec_time = end_exec - start_exec
    exec_mins = exec_time / 60
    docs_per_sec = total_msgs / exec_time if total_msgs > 0 else 0
    
    logger.info('Found %d messages in %.2f minutes, docs/sec: %.0f', total_msgs, exec_mins, docs_per_sec)
    
    return total_msgs

# Initialize the query for slicing and ES 7.
def init_query(query_body):
    # Needed for all ES versions.
    query_body.update(json.loads(JSON_SLICE))    
    query_body.update(json.loads(JSON_SIZE))    
    query_body.update(json.loads(JSON_SORT))
    
    if es_version == VERSION_ES5: 
        return query_body
        
    # The following are for ES7+.
    query_body.update(json.loads(JSON_TRACK_TOTAL_HITS))
    
    # "query" must be present.
    if 'query' not in query_body:
        raise Exception('JSON query is missing the "query" field.')
        
    # "bool" must be present.
    if 'bool' not in query_body['query']:
        raise Exception('JSON query is missing the "bool" field.')
      
    # Add "filter" if missing.  
    if 'filter' not in query_body['query']['bool']:
        filter = list()
        query_body['query']['bool']['filter'] = filter

    # Add doc_type filter.
    query_body['query']['bool']['filter'].append(json.loads(JSON_DOC_TYPE_FILTER))
    
    return query_body
 
# Get the requested source fields.           
def get_source_fields(query_body):
    if '_source' in query_body and 'includes' in query_body['_source']:
        return query_body['_source']['includes']
            
    return list()
    
# Start of main.
def main(args):
    global logger
    
    logging.basicConfig(format='[%(asctime)s][%(name)s][%(levelname)s] %(message)s')
    logger = logging.getLogger('main')
    logger.setLevel(args.logging_level)

    query_file = args.query_file
    index_pattern = args.index_pattern 
    
    logger.info('Query file: %s', query_file)
    logger.info('Index pattern: %s', index_pattern)
    
    # Silence https warnings
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    
    check_es_version()  
    
    logger.info('ES version: %s', es_version)
       
    with open(query_file, 'r', encoding='utf-8') as f:
        query_body = json.load(f)  
        logger.debug('Loaded ES query: %s', json.dumps(query_body))
    
    query_body = init_query(query_body)  
    logger.debug('Inited ES query body: %s', json.dumps(query_body))
    
    scroll_body = json.loads(JSON_SCROLL_BODY)
    
    source_fields = get_source_fields(query_body)           
    
    logger.info('System fields: %s', SYSTEM_FIELDS)
    logger.info('Source fields: %s', source_fields)
    
    if len(SYSTEM_FIELDS) + len(source_fields) == 0:
        logger.info('No fields configured to be saved.')
        
    else:   
        total_msgs = save_message_metadata_to_csv(index_pattern, query_body, scroll_body, source_fields) 
        logger.info('Saved %d messages.', total_msgs)
                                       
    logger.info('Done.')                  
    
if __name__ == '__main__':    
    parser = argparse.ArgumentParser(description='Search and save message metadata to a CSV file for one or more indices.')
    parser.add_argument('query_file', help='JSON file containing the ES query')
    parser.add_argument('index_pattern', help='ES index pattern')
    
    parser.add_argument('-l', '--logging-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='log level (default: %(default)s)')
    main(parser.parse_args())
