import os

from databases import Database
from sanic import Sanic
from sanic import response

app = Sanic("IMDB Movie Reviews")

app.db = Database('sqlite:////{}/movies.db'.format(os.getcwd()))


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


@app.listener('after_server_stop')
async def disconnect_from_db(*args, **kwargs):
    print("Disconnecting with the Database!!")
    await app.db.disconnect()


@app.route("/")
def run(request):
    return response.text("Hello World !")


app.run(host="0.0.0.0", port=8000, debug=True)
