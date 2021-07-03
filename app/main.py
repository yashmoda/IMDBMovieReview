import json
import os

from databases import Database
from sanic import Sanic
from sanic import response

app = Sanic("IMDB Movie Reviews")

app.db = Database('sqlite:////{}/movies.db'.format(os.getcwd()))


async def load_genres(genre):
    genre_query = """SELECT GENRE FROM GENRE"""
    genre_res = await app.db.fetch_all(genre_query)
    genre_res = [g[0] for g in genre_res]
    if len(genre) == 0:
        return
    res = []
    for g in genre:
        if g not in genre_res:
            temp_json = {'genre': g}
            res.append(temp_json)
    genre_query = """INSERT INTO GENRE
                            (GENRE) 
                            VALUES 
                            (:genre)
                            """
    # print(res)
    await app.db.execute_many(genre_query, res)


async def match_movie_genre(movie_name):
    genre_dict = {}
    movie_genre = []
    genre_query = "SELECT ID, GENRE FROM GENRE"
    genre_res = await app.db.fetch_all(genre_query)
    for genre in genre_res:
        genre_dict[genre[1]] = genre[0]
    print(movie_name)
    for key, value in movie_name.items():
        movie_genre_query = """SELECT M.ID FROM MOVIES M
                                WHERE M.NAME = :name
                                """
        count = await app.db.fetch_one(movie_genre_query, values={"name": key})
        movie_id = count[0]
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
        # print(movie_genre)


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
            await insert_movie(movie_list)

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
    # await load_movies()


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


@app.route("/add/", methods=["POST"])
async def add_movie(request):
    try:
        username = request.form.get("username")
        if await is_admin(username):
            name = request.form.get("name")
            director = request.form.get("director")
            imdb_score = request.form.get("imdb_score")
            popularity = request.form.get("popularity")
            genre = request.form.get("genre").split(",")
            genre = set([g.strip() for g in genre])
            movie_dict = {"name": name,
                          "director": director,
                          "imdb_score": imdb_score,
                          "popularity": popularity}
            await load_genres(genre)
            await insert_movie(movie_dict)
            await match_movie_genre({name: genre})
            return response.text("The movie has been successfully added.")
        else:
            return response.text("You are not the admin. Only admin can add a movie!!")
    except Exception as e:
        print(str(e))
        return response.text(str(e))


@app.route("/delete/", methods=["DELETE"])
async def delete_movie(request):
    try:
        username = request.args.get("username")
        print(username)
        if await is_admin(username):
            name = request.args.get("name")
            print(name)
            movie_id_query = """SELECT ID FROM MOVIES WHERE name = :name """
            movie_id = await app.db.fetch_one(movie_id_query, values={"name": name})
            movie_id = movie_id[0]

            print(movie_id)

            movie_genre_query = """DELETE FROM MOVIE_GENRE WHERE MOVIE_ID = :id """
            await app.db.execute(movie_genre_query, values={"id": movie_id})

            movie_query = """DELETE FROM MOVIES WHERE ID = :id """
            await app.db.execute(movie_query, values={"id": movie_id})

            return response.text("The movie has been successfully deleted!!")
        else:
            return response.text("You are not an admin!! Only an admin can delete a movie.")
    except Exception as e:
        print(str(e))
        return response.text("An error has occurred. Please try again later")


@app.route("/add_user/", methods=["POST"])
async def add_user(request):
    try:
        username = request.form.get("username")
        is_admin = request.form.get("is_admin")
        user = {"username": username.lower(),
                "is_admin": is_admin.upper()}
        await insert_user(user)
        return response.text("The user has been added successfully")
    except Exception as e:
        return response.text(str(e))


async def is_admin(username):
    query = """SELECT IS_ADMIN FROM USERS WHERE USERNAME = :name"""
    admin = await app.db.fetch_one(query, values={"name": username})
    return True if admin is not None and admin[0].upper() == 'Y' else False


async def insert_user(user):
    query = """
                INSERT INTO USERS
                (USERNAME, IS_ADMIN)
                VALUES
                (:username, :is_admin)
            """
    await app.db.execute(query, values=user)
    print("Inserted!!")
    return


async def insert_movie(movie):
    movie_query = """INSERT INTO MOVIES
                    (NAME, DIRECTOR, POPULARITY, IMDB_SCORE) 
                    VALUES 
                    (:name, :director, :popularity, :imdb_score)
                    """
    await app.db.execute(movie_query, movie)
    # print(movie)


app.run(host="0.0.0.0", port=8000, debug=True)
