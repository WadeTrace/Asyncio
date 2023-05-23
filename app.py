import asyncio
import datetime

import aiohttp
from more_itertools import chunked
from model import Base, SwapiPeople, Session, engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession


MAX_REQUESTS = 5


async def get_film_name(client, url):
    async with client.get(url) as response:
        json_data = await response.json()
        return json_data['title']


async def get_species_name(client, url):
    async with client.get(url) as response:
        json_data = await response.json()
        return json_data['name']


async def get_starship_name(client, url):
    async with client.get(url) as response:
        json_data = await response.json()
        return json_data['name']


async def get_vehicle_name(client, url):
    async with client.get(url) as response:
        json_data = await response.json()
        return json_data['name']


async def get_people(client, people_id):
    async with client.get(f'https://swapi.dev/api/people/{people_id}') as response:
        if response.status == 200:
            json_data = await response.json()

            data = {}
            data['birth_year'] = json_data.get('birth_year')
            data['eye_color'] = json_data.get('eye_color')

            films_coros = [get_film_name(client, url) for url in json_data.get('films')]
            result = await asyncio.gather(*films_coros)
            data['films'] = ','.join(result)

            data['gender'] = json_data.get('gender')
            data['hair_color'] = json_data.get('hair_color')
            data['height'] = json_data.get('height')
            data['homeworld'] = json_data.get('homeworld')
            data['mass'] = json_data.get('mass')
            data['name'] = json_data.get('name')
            data['skin_color'] = json_data.get('skin_color')

            species_coros = [get_species_name(client, url) for url in json_data.get('species')]
            result = await asyncio.gather(*species_coros)
            data['species'] = ','.join(result)

            starships_coros = [get_starship_name(client, url) for url in json_data.get('starships')]
            result = await asyncio.gather(*starships_coros)
            data['starships'] = ','.join(result)

            vehicles_coros = [get_vehicle_name(client, url) for url in json_data.get('vehicles')]
            result = await asyncio.gather(*vehicles_coros)
            data['vehicles'] = ','.join(result)

            return data
        else:
            return None


async def paste_to_db(people_jsons):
    async with Session() as session:
        orm_objects = [SwapiPeople(**item) for item in people_jsons if item is not None]
        session.add_all(orm_objects)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    tasks = []
    async with aiohttp.ClientSession() as client:
        for people_id_chunk in chunked(range(1, 10), MAX_REQUESTS):

            person_coros = [get_people(client, people_id) for people_id in people_id_chunk]
            result = await asyncio.gather(*person_coros)
            paste_to_db_coro = paste_to_db(result)
            paste_to_db_task = asyncio.create_task(paste_to_db_coro)
            tasks.append(paste_to_db_task)

    tasks = asyncio.all_tasks() - {asyncio.current_task(), }
    for task in tasks:
        await task

    print('finish')


if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
