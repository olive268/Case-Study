
from flask import Flask, jsonify,request
from pymongo import MongoClient
import json
from bson import json_util, ObjectId

app = Flask(__name__)
app.config["DEBUG"] = True
app.config["TESTING"] = True



client = MongoClient("mongodb://localhost:27017")

'''Establishing a DB and collection instance'''
db=client.DB
tasks_collection1 = db.CountryAndCount
tasks_collection2= db.CountryCountAndDate
tasks_collection3 = db.WordCount
tasks_collection4 = db.WordCountCountry


'''Query 1: Finding count of overall tweets per country'''
@app.route('/tweets_per_country', methods=['GET'])
def tweets_per_country():
    cursor = tasks_collection1.aggregate([
        {
            '$sort' : {'count' : -1}
        }
    ],allowDiskUse=True)
    ans = []
    for doc in cursor:
        doc.pop('_id')
        ans.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans)


'''Query 1 - Subquery: Finding count of overall tweets per country for a specific country'''
@app.route('/tweets_per_country/<country_name>', methods=['GET'])
def query11(country_name):

    '''country fix'''
    new_country=str(country_name).replace("_"," ").title()

    cursor = tasks_collection1.aggregate([
        {
            '$match':{
                'country':new_country
            }
        }
    ],allowDiskUse=True)

    ans = []
    for doc in cursor:
        doc.pop('_id')
        ans.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans)



'''Query 2: Finding count of tweets per country on daily basis'''
@app.route('/tweets_per_country_date', methods=['GET'])
def query2():
    cursor1 = tasks_collection2.aggregate([

        {
            '$sort' : {'country' : 1}
        }
    ],
        allowDiskUse=True
    )

    ans = []
    for doc in cursor1:
        doc.pop('_id')
        ans.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans)


'''Query 2 - Subquery: Finding count of tweets per country'''
@app.route('/tweets_per_country_date/<country_name>', methods=['GET'])
def query21(country_name):

    '''country fix'''
    new_country=str(country_name).replace("_"," ").title()

    cursor21 = tasks_collection2.aggregate([
        {
            '$match':{
                'country':new_country
            }
        },

        {
            '$sort' : {'count' : -1}
        }
    ],
        allowDiskUse=True
    )

    ans = []
    for doc in cursor21:
        doc.pop('_id')
        ans.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans)

'''Query 2 - Subquery: Finding count of tweets per date'''
@app.route('/tweets_per_country_date/date/<date>', methods=['GET'])
def query22(date):


    '''date fix'''
    new_date=str(date).replace("_"," ").capitalize()
    new_date1=new_date[:6]+","+new_date[6:]

    cursor21 = tasks_collection2.aggregate([
        {
            '$match':{
                'date':new_date1
            }
        },

        {
            '$sort' : {'count' : -1}
        }
    ],
        allowDiskUse=True
    )

    ans = []
    for doc in cursor21:
        doc.pop('_id')
        ans.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans)


'''Query 2 - Subquery: Finding count of tweets per date'''
@app.route('/tweets_per_country_date/date_country/<country_name>/<date>', methods=['GET'])
def query23(country_name,date):

    '''country fix'''
    new_country=str(country_name).replace("_"," ").title()

    '''date fix'''
    new_date=str(date).replace("_"," ").capitalize()
    new_date1=new_date[:6]+","+new_date[6:]

    cursor21 = tasks_collection2.aggregate([
        {
            '$match':{
                'country':new_country,
                'date':new_date1
            }
        }
    ],
        allowDiskUse=True
    )

    ans = []
    for doc in cursor21:
        doc.pop('_id')
        ans.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans)


@app.route('/overall_top_100words', methods=['GET'])
def query3():
    cursor3 = tasks_collection3.aggregate([
        {
            '$sort':{
                'count':-1
            }
        },
        {
            '$limit':100
        }


    ],
        allowDiskUse=True
    )

    ans = []
    for doc in cursor3:
        doc.pop('_id')
        ans.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans)


@app.route('/top_100words_country', methods=['GET'])
def query4():
    cursor = tasks_collection4.aggregate([

        {
            '$group':{
                '_id':"$country",
                'top100_words_forThisCountry':{
                    '$push':"$word"
                }
            }
        },
        {
            '$sort':{
                '_id':1
            }
        },
        {
            '$project':{
                'country':1,
                'top100_words_forThisCountry':{
                    '$slice':[ "$top100_words_forThisCountry", 100 ]
                }
            }
        }


    ],

        allowDiskUse=True
    )

    ans = []
    for doc in cursor:
        ans.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans)

@app.route('/top_100words_country/<country_name>', methods=['GET'])
def query4_1(country_name):

    '''country fix'''
    new_country=str(country_name).replace("_"," ").title()

    cursor = tasks_collection4.aggregate([
        {
            '$match':{
                'country':new_country
            }
        },

        {
            '$group':{
                '_id':"$country",
                'top100_words_forThisCountry':{
                    '$push':"$word"
                }
            }
        },
        {
            '$project':{
                'country':1,
                'top100_words_forThisCountry':{
                    '$slice':[ "$top100_words_forThisCountry", 100 ]
                }
            }
        },
        {
            '$sort':{
                '_id':1
            }
        }

    ],

        allowDiskUse=True
    )

    ans = []
    for doc in cursor:
        ans.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans)


@app.route('/overall_donations_country', methods=['GET'])
def query6():
    cursor5 = tasks_collection4.aggregate([
        {
            '$match':{
                "word":{
                    '$in':[ "donated","donate","donation","contribute"]
                }
            }
        },
        {
            '$project':{
                'country':1,
                'count':1
            }
        },
        {
            '$group':{
                '_id':'$country',
                'count_per_country':{'$sum':'$count'}
            }
        },
        {
            '$sort':{
                '_id':1
            }
        }

    ],

        allowDiskUse=True
    )

    ans5 = []
    for doc in cursor5:
        ans5.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans5)




@app.route('/overall_donations_country/<country_name>', methods=['GET'])
def query61(country_name):

    '''country fix'''
    new_country=str(country_name).replace("_"," ").title()
    cursor5 = tasks_collection4.aggregate([
        {
            '$match':{
                'country':new_country
            }
        },
        {
            '$match':{
                "word":{
                    '$in':[ "donated","donate","donation","contribute"]
                }
            }
        },
        {
            '$project':{
                'country':1,
                'count':1
            }
        },
        {
            '$group':{
                '_id':'$country',
                'count_per_country':{'$sum':'$count'}
            }
        },

    ],

        allowDiskUse=True
    )

    ans5 = []
    for doc in cursor5:
        ans5.append(json.loads(json_util.dumps(doc)))

    return jsonify(ans5)





@app.route('/', methods=['GET'])
def queryMain():
    return "Welcome"

@app.errorhandler(404)
def page_not_found(error):
    return 'This page does not exist , please enter a valid url', 404

@app.errorhandler(500)
def special_exception_handler(error):
    return 'Database connection failed', 500

app.run()

