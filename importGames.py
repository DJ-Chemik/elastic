import csv
from collections import deque
import elasticsearch
from elasticsearch import helpers

def readGames():
    csvfile = open('games.csv', 'r', encoding="utf8")

    reader = csv.DictReader( csvfile )

    for line in reader:
        game = {}
        game['name'] = line['Name']
        game['platform'] = line['Platform']
        try: 
            game['year'] = int(line['Year_of_Release'])
        except:
            game['year'] = -1
        game['genre'] = line['Genre']
        game['publisher'] = line['Publisher']
        game['na_sales'] = line['NA_Sales']
        game['eu_sales'] = line['EU_Sales']
        game['jp_sales'] = line['JP_Sales']
        game['other_sales'] = line['Other_Sales']
        game['global_sales'] = line['Global_Sales']
        game['critic_score'] = line['Critic_Score']
        game['critic_count'] = line['Critic_Count']
        game['user_score'] = line['User_Score']
        game['user_count'] = line['User_Count']
        game['developer'] = line['Developer']
        game['rating'] = line['Rating']
        yield game

es = elasticsearch.Elasticsearch(["http://127.0.0.1:9200"])
es.indices.delete(index="ratings", ignore=404)
deque(helpers.parallel_bulk(es,readGames(),index="games", request_timeout=300), maxlen=0)
es.indices.refresh()
