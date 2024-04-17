# Gayan_Repo

> This is an ES script to fetch a large set of data using scroll query. 
> Download the csurv-export-metadata.py
> Download the msg_error_query.json
> Open the script using any of your IDE or notepad and set the ES host (localhost) to an ES data node hostname or IP address and set the ES password at the top of the script.

> Run the python scipt as below
>>** python3 csurv-export-metadata.py msg_error_query.json santander_prod__message **<<
>NOTE: santander_prod__message searches all alerted and non-alerted indices. You can also pick which indices to search with wildcards. Example, search non-alerted only for 2023... "santander_prod__message_types_2023*"
