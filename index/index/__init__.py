"""Index Server Initializer."""
import os
import flask

# app is a single object used by all the code modules in this package
app = flask.Flask(__name__)

app.config["INDEX_PATH"] = os.getenv("INDEX_PATH")
app.config["EMPTY_HITS"] = {"hits": []}

# Tell our app about api and inverted_index.
import index.api  # noqa: E402  pylint: disable=wrong-import-position
