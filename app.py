from flask import Flask,redirect,render_template,jsonify

app = Flask(__name__)

@app.route('/')
def index():
    return "consumer_complaints_analysis_project"


if __name__=="__main__":
    app.run(debug=True)
    