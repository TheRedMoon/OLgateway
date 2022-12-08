from flask import Flask
from flask_restful import Resource, Api, reqparse
import ast
from flask import request,jsonify
from gateway import handle_openlineage_record
app = Flask(__name__)
api = Api(app)

@app.route('/bert', methods = ['POST'])
def user():
    if request.method == 'POST':
        data = request.get_json()
        print(f'Data received{data}')
        handle_openlineage_record(data)

    return jsonify(isError= False,
                        message= "Success",
                        statusCode= 200,
                        data= data), 200

if __name__ == '__main__':
    app.run()  # run our Flask app
