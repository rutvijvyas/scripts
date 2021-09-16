import argparse
import csv
import logging
import sys
import time
import math
from neo4j import GraphDatabase

rows_param_name = "rows"
batch_size = 1000
database = "neo4j"
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(message)s",
    handlers = [ logging.StreamHandler(sys.stdout) ]
)
driver = None

def get_argument_parser():
    parser = argparse.ArgumentParser(description="Arguments for loading csv into neo4j database")
    parser.add_argument("-n", "--neo4juri", type=str, help="Neo4j neo4juri(mandatory)", required=True)
    parser.add_argument("-d", "--database", type=str, help="Neo4j database")
    parser.add_argument("-u", "--username", type=str, help="Neo4j username(mandatory)", required=True)
    parser.add_argument("-p", "--password", type=str, help="Neo4j password(mandatory)", required=True)
    parser.add_argument("-c","--cypher", type=str, help="Input cypher query(mandatory)", required=True)
    parser.add_argument("-f", "--file", type=str, help="Input CSV file path")
    parser.add_argument("-b", "--batch", type=int, help="Batch Size")
    return parser

def get_neo4j_connection(neo4juri, username, password):
    logging.info(f"Connecting to Neo4j {neo4juri}")
    return GraphDatabase.driver(uri=neo4juri, auth=(username, password))

def execute_cypher(cypher, file):
    logging.info(f"Loading values from {file} to params")
    data = read_csv_to_dict_list(file)
    size = len(data)
    batches = int(math.ceil(len(data)/batch_size))
    for i in range (0, batches) :
        start = i * batch_size
        end = (start + batch_size)
        if end > size :
            end = size
        logging.info(f"Batch - Start: '{start}' End: '{end}'")
        run_cypher(cypher, {rows_param_name: data[start:end]})

def run_cypher(cypher, params=None):
    with driver.session(database=database) as session:
        start = time.perf_counter()
        logging.info(f"Executing cypher '{cypher}'")
        try:
            result = session.write_transaction(write_transaction, cypher, params)
            end = time.perf_counter()
            time_taken = round((end - start) * 1000, 6)
            logging.info(f"Counters: {result}, TimeTaken: {time_taken} ms")
        except Exception as e:
            logging.exception(f"Exception executing the cypher '{cypher}'")
            sys.exit(1)

def write_transaction(tx, cypher, params):
	return tx.run(cypher, params).consume().counters

def read_csv_to_dict_list(csv_file):
    with open(csv_file, "r", encoding="utf-8") as f:
        return [
            {k: v for k, v in row.items()}
            for row in csv.DictReader(f, skipinitialspace=True)
        ]

if __name__ == '__main__':
    parser = get_argument_parser()
    args = parser.parse_args()
    neo4juri = args.neo4juri
    if args.database:
        database = args.database
    username = args.username
    password = args.password
    cypher = args.cypher
    file = args.file
    if args.batch:
        batch_size = args.batch

    driver = get_neo4j_connection(neo4juri, username, password)

    execute_cypher(cypher, file)