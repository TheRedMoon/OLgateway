from flask import Flask
from flask_restful import Resource, Api, reqparse
import ast
from flask import request,jsonify
from gateway import handle_openlineage_record
app = Flask(__name__)
api = Api(app)


class Users(Resource):
    # methods go here
    pass

api.add_resource(Users, '/users')  # '/users' is our entry point


@app.route('/bert', methods = ['POST'])
def user():
    if request.method == 'POST':
        """modify/update the information for <user_id>"""
        # you can use <user_id>, which is a str but could
        # changed to be int or whatever you want, along
        # with your lxml knowledge to make the required
        # changes
        data = request.get_json()
        print(data)
        handle_openlineage_record(data)

    return jsonify(isError= False,
                        message= "Success",
                        statusCode= 200,
                        data= data), 200

if __name__ == '__main__':
    app.run()  # run our Flask app
