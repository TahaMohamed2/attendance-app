from flask import Flask, render_template, request

from attendance import  attendance_main, extract_file_id

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        url = request.form.get('url')
        file_id=extract_file_id(url)
        #att_table=attendance_main(url)   
        return render_template('submit.html', url_input=url, file_id=file_id) #, result=att_table)
    return render_template('index.html')



# Main execution flow
if __name__ == '__main__':
    app.run(debug=True)
