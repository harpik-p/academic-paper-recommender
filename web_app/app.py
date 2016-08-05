# import the Flask class from the flask module
from flask import Flask, render_template, request
from flaskext.mysql import MySQL
import pandas as pd

# create the application object
mysql = MySQL()
app = Flask(__name__)
app.config['MYSQL_DATABASE_USER'] = 'root'
#app.config['MYSQL_DATABASE_PASSWORD'] = 'root'
app.config['MYSQL_DATABASE_DB'] = 'rec_db'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql.init_app(app)

# use decorators to link the function to a url
@app.route('/')
def home():
    return render_template('index.html', form_action="/recommender#search")

@app.route('/recommender', methods=['POST'])
def recommender():
    pmid = str(request.form['pmid'])

    cursor = mysql.connect().cursor()
    query_recommendations = "SELECT p.pmid, p.title, p.journal_name, p.year, r.score " \
            "FROM paper p " \
            "INNER JOIN " \
            "(SELECT recommended_pmid as pmid, sum(51-rank) as score " \
            "FROM recommendation " \
            "WHERE pmid=" + pmid +" GROUP BY recommended_pmid ORDER BY score DESC) r on p.pmid=r.pmid;"

    query_pmid = "SELECT pmid, title, journal_name, year FROM paper WHERE pmid=" + pmid + ";"

    try:
        cursor.execute(query_recommendations)
        data = cursor.fetchall()
        df_recommendations = pd.DataFrame(list(data), columns=['PMID', "Title", "Journal", "Year", "Score"])
        rec_result = df_recommendations

        cursor.execute(query_pmid)
        pmid_info = cursor.fetchall()
        df_pmid = pd.DataFrame(list(pmid_info), columns=['PMID', "Title", "Journal", "Year"])
        pmid_result = df_pmid

        if data:
            return render_template('recommender.html', msg="", df=rec_result, df_pmid=pmid_result,
                                   form_action="/recommender#search")

        else:
            return render_template('index.html',
                                   msg="Unfortunately, this paper is not in our database. Please try another paper.",
                                   form_action='/recommender#search')

    except:
        return render_template('index.html',
                           msg="Please enter a valid paper id.",
                           form_action='/recommender#search')



# start the server with the 'run()' method
if __name__ == '__main__':
    app.run(debug=True)