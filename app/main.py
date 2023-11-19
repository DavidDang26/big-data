from flask import Flask, Response
from generate_file import get_csv_url

app = Flask(__name__)


@app.route("/")
def hello_world():
    url = get_csv_url()
    return Response(response=url, status=200)


if __name__ == "__main__":
    app.run(debug=True)
