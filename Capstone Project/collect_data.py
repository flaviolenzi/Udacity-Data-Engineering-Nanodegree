import json
import pandas as pd
from requests import get as requestget
import requests

# stackoverflow Application Key
Key = ''

# Per-Site Methods
questions_api = 'https://api.stackexchange.com/2.3/questions?page={}&pagesize=100&order=asc&sort=creation&site=stackoverflow&key={}'
users_api = 'https://api.stackexchange.com/2.3/users?page={}&pagesize=100&order=asc&sort=creation&site=stackoverflow&key={}'
tags_api = 'https://api.stackexchange.com/2.3/tags?page={}&pagesize=100&order=desc&sort=popular&site=stackoverflow&key={}'


def get_data_from_api(number_requests, dataset, output_folder): # A simple function to use requests.post to make the API call.
    """Get data from API

    :param pages_range: Number of pages to request
    :param dataset: Name of the dataset
    :param output_folder: Name of the folder to load the retrived data
    """
    for page in range(1, number_requests):
        request = requests.get(dataset.format(page, Key))
        arquivo = json.loads(request.text)
        #print(arquivo)
        with open('./data/{}/{}_{}.json'.format(output_folder, output_folder, page), 'a') as f:
            f.write(json.dumps(arquivo['items']))



def main():
    get_data_from_api(101, tags_api, 'tags')
    get_data_from_api(101, users_api, 'users')
    get_data_from_api(101, questions_api, 'questions')


if __name__ == "__main__":
    main()