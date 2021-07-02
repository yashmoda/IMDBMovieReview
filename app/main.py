import json
import os

from databases import Database
from sanic import Sanic
from sanic import response

app = Sanic("IMDB Movie Reviews")

app.db = Database('sqlite:////{}/movies.db'.format(os.getcwd()))


async def load_movies():
    query = """SELECT COUNT(*) FROM MOVIES"""
    movie_count = await app.db.fetch_one(query)
    genre = set()
    movie_list = []
    file = open('imdb.json', 'r')
    data = json.load(file)
    for i in data:
        temp_json = {
            'name': i['name'].strip(),
            'director': i['director'].strip(),
            'popularity': i['99popularity'],
            'imdb_score': i['imdb_score']
        }
        movie_list.append(temp_json)

        for j in i['genre']:
            g = j.strip()
            genre.add(g)
    if movie_count[0] == 0:
        movie_query = """INSERT INTO MOVIES
                        (NAME, DIRECTOR, POPULARITY, IMDB_SCORE) 
                        VALUES 
                        (:name, :director, :popularity, :imdb_score)
                        """
        await app.db.execute_many(movie_query, movie_list)
        query = """SELECT COUNT(*) FROM MOVIES"""

        movie_count = await app.db.fetch_one(query)
        print(movie_count)
    genre_query = """SELECT COUNT(*) FROM GENRE"""
    genre_count = await app.db.fetch_one(genre_query)
    if genre_count[0] == 0:
        res = []
        for g in genre:
            temp_json = {'genre': g}
            res.append(temp_json)
        genre_query = """INSERT INTO GENRE
                        (GENRE) 
                        VALUES 
                        (:genre)
                        """
        await app.db.execute_many(genre_query, res)
        genre_query = """SELECT COUNT(*) FROM GENRE"""
        genre_count = await app.db.fetch_one(genre_query)
        print(genre_count)


@app.listener('after_server_start')
async def connect_to_db(*args, **kwargs):
    print("Connecting to DB!!")
    await app.db.connect()

    query = """CREATE TABLE IF NOT EXISTS MOVIES 
                    (ID INTEGER PRIMARY KEY, 
                    NAME VARCHAR(100),
                    DIRECTOR VARCHAR(100),
                    POPULARITY DECIMAL(4,1),
                    IMDB_SCORE DECIMAL(3,1)
            )"""
    await app.db.execute(query=query)
    print("!!!Created Movies Table!!!")

    query = """CREATE TABLE IF NOT EXISTS GENRE
                    (ID INTEGER PRIMARY KEY,
                    GENRE VARCHAR(50),
                    UNIQUE (GENRE)
            )"""
    await app.db.execute(query=query)
    print("!!!Created Genre Table!!!")

    query = """CREATE TABLE IF NOT EXISTS MOVIE_GENRE
                    (ID INTEGER PRIMARY KEY,
                    MOVIE_ID INTEGER,
                    GENRE_ID INTEGER,
                    FOREIGN KEY (MOVIE_ID) REFERENCES MOVIES(ID),
                    FOREIGN KEY (GENRE_ID) REFERENCES GENRE(ID)
            )"""
    await app.db.execute(query=query)
    print("!!!Created Movie Genre Table!!!")

    query = """CREATE TABLE IF NOT EXISTS USERS
                    ( ID INTEGER PRIMARY KEY,
                    USERNAME VARCHAR (50),
                    IS_ADMIN CHAR(1) DEFAULT 'N',
                    UNIQUE (USERNAME)
            )"""
    await app.db.execute(query=query)
    print("!!!Created User Table!!!")
    await load_movies()


@app.listener('after_server_stop')
async def disconnect_from_db(*args, **kwargs):
    print("Disconnecting with the Database!!")
    await app.db.disconnect()


@app.route("/")
def run(request):
    return response.text("Hello World !")


async def is_admin(username):
    query = """SELECT IS_ADMIN FROM USERS WHERE USERNAME = {}""".format(username)
    admin = await app.db.fetch_one(query)
    return True if admin[0] == 'Y' else False


app.run(host="0.0.0.0", port=8000, debug=True)
