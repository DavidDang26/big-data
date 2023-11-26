from flask import Flask, Response
from generate_file import get_json_result

app = Flask(__name__)

@app.route("/")
def process():
    json = get_json_result()
    return Response(response=json, status=200)


if __name__ == "__main__":
    app.run(debug=True)
