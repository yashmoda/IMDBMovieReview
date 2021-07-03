import json
import os

from databases import Database
from sanic import Sanic
from sanic import response

app = Sanic("IMDB Movie Reviews")

app.db = Database('sqlite:////{}/movies.db'.format(os.getcwd()))


async def load_genres(genre):
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


async def match_movie_genre(movie_name):
    movie_genre_query = """SELECT COUNT(*) FROM MOVIE_GENRE"""
    count = await app.db.fetch_one(movie_genre_query)
    genre_dict = {}
    movie_genre = []
    if count[0] == 0:
        genre_query = "SELECT ID, GENRE FROM GENRE"
        genre_res = await app.db.fetch_all(genre_query)
        for genre in genre_res:
            genre_dict[genre[1]] = genre[0]
        for key, value in movie_name.items():
            movie_id_query = """SELECT ID FROM MOVIES WHERE NAME = :name"""
            movie_id_res = await app.db.fetch_one(movie_id_query, values={"name": key})
            movie_id = movie_id_res[0]

            for val in value:
                temp_json = {
                    'movie_id': movie_id,
                    'genre_id': genre_dict.get(val)
                }
                movie_genre.append(temp_json)
            insert_query = """INSERT INTO MOVIE_GENRE
                                (MOVIE_ID, GENRE_ID)
                                VALUES
                                (:movie_id, :genre_id)"""
            await app.db.execute_many(insert_query, movie_genre)


async def load_movies():
    try:
        query = """SELECT COUNT(*) FROM MOVIES"""
        movie_count = await app.db.fetch_one(query)
        genre = set()
        movie_list = []
        file = open('imdb.json', 'r')
        data = json.load(file)
        movie_name = {}
        for i in data:
            temp_json = {
                'name': i['name'].strip(),
                'director': i['director'].strip(),
                'popularity': i['99popularity'],
                'imdb_score': i['imdb_score']
            }
            if i["name"].strip() not in movie_name:
                movie_name[i["name"].strip()] = []

            movie_list.append(temp_json)

            for j in i['genre']:
                g = j.strip()
                movie_name[i["name"].strip()].append(g)
                genre.add(g)
        if movie_count[0] == 0:
            movie_query = """INSERT INTO MOVIES
                            (NAME, DIRECTOR, POPULARITY, IMDB_SCORE) 
                            VALUES 
                            (:name, :director, :popularity, :imdb_score)
                            """
            await app.db.execute_many(movie_query, movie_list)

        await load_genres(genre)
        await match_movie_genre(movie_name)

    except Exception as e:
        print(str(e))


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


@app.route("/", methods=["GET"])
async def get_reviews(request):
    query = request.args.get("query")
    search_query = """SELECT M.ID, M.NAME, M.DIRECTOR, M.IMDB_SCORE, M.POPULARITY, G.GENRE
                        FROM MOVIES M
                        INNER JOIN MOVIE_GENRE MG
                            ON M.ID = MG.MOVIE_ID
                        INNER JOIN GENRE G
                            ON G.ID = MG.GENRE_ID
                        WHERE M.NAME LIKE :name"""
    result = await app.db.fetch_all(search_query, values={"name": "%{}%".format(query)})
    response_val = {}
    for row in result:
        if row[0] not in response_val:
            response_val[row[0]] = {}
        if 'name' not in response_val[row[0]]:
            response_val[row[0]]['name'] = row[1]
        if 'director' not in response_val[row[0]]:
            response_val[row[0]]['director'] = row[2]
        if 'imdb_score' not in response_val[row[0]]:
            response_val[row[0]]['imdb_score'] = row[3]
        if 'popularity' not in response_val[row[0]]:
            response_val[row[0]]['popularity'] = row[4]
        if 'genre' not in response_val[row[0]]:
            response_val[row[0]]['genre'] = [row[5]]
        else:
            if row[5] not in response_val[row[0]]['genre']:
                response_val[row[0]]['genre'].append(row[5])

    return response.json(response_val)


async def is_admin(username):
    query = """SELECT IS_ADMIN FROM USERS WHERE USERNAME = {}""".format(username)
    admin = await app.db.fetch_one(query)
    return True if admin[0] == 'Y' else False


app.run(host="0.0.0.0", port=8000, debug=True)
