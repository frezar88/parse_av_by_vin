import random
from time import sleep

import requests
from mysql.connector import connect

counter = 0

api_keys = ['x0e24798cf9429dc0891e28',
            'md95d5da92b50d47ea0c495',
            'ccea327a1a412c058208ee7',
            'x6f3763e079c37714c9bef1',
            'jc579ef830f1ddb1392397d', 'j900d5546da20b67d01d9a2', 'ce26208f8c4fafdb7401eb2',
            'h058d51d0202c5fadfab743',
            's748ab69083226079d953aa', 'l4f1435725534e908e8dd68', 've1d7a5178adee247589c17',
            'z8761eb8d91c8aa3fa09e0e',
            'y2bf697daa722f1aa416fba',
            'cbe09c1d47f244995aa5d93',
            'gce4bfa2497c3d4d29aae27',
            'c54ce0859faafadd1fe8e93',
            'h4f86ed71f71797914aef20', 'e4688fb8a4bd1261ec2fde2', 'f5e958fb30ee4e625d44b42',
            'of3aab0105b90f88904f67d',
            'sd4eeb9f3dd5d7390012ce4'
            ]


def get_vin(car_id):
    global counter
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,ru;q=0.8,ru-RU;q=0.7",
        "Cache-Control": "no-cache, private",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36",
        'x-api-key': api_keys[counter:][0]
    }

    url = f'https://api.av.by/offer-types/cars/offers/{car_id}/vin'
    sleep(random.randint(0, 1))
    r = requests.get(url=url, headers=headers)
    if r.status_code == 200:
        print(r.json())
        return r.json()
    else:
        print(r.json().get('messageText'))
        if len(api_keys) - 1 > counter:
            counter += 1
            return get_vin(car_id)
        else:
            print('На сегодня всё. Закончился лимит')
            return False


def get_data_cars():
    mydb = connect(
        host="localhost",
        user="parser_av",
        password="QAn2kE67S6S6VEM73uL2Ju2OGeti5O",
        database="parser_av"
        # host="localhost",
        # user="root",
        # password="41111",
        # database="parse_av"
    )

    my_cursor = mydb.cursor()
    my_cursor.execute('START TRANSACTION')
    # my_cursor.execute(f"SELECT id,car_id,is_vin,vin FROM cars  WHERE vin IS NULL AND is_vin = 1")
    my_cursor.execute(
        f"SELECT * FROM cars JOIN cars_info ON cars.car_id = cars_info.car_id AND cars.vin IS NULL AND cars.is_vin = 1 AND cars_info.photo_local_path IS NULL AND cars.status='active'")
    my_result = my_cursor.fetchall()
    photo_item_name = 0
    for item in my_result:
        photo_item_name += 1
        sleep(random.randint(0, 2))
        vin = get_vin(item[1])
        if not vin:
            break

        sql_vin = f"UPDATE cars  SET is_vin = 1, vin = '{vin.get('vin')}' WHERE car_id = {item[1]}"
        my_cursor.execute(sql_vin)
        my_cursor.execute('COMMIT')

    my_cursor.close()


def main():
    get_data_cars()


if __name__ == __name__:
    main()
