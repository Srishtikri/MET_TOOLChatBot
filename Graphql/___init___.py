import os
import azure.functions as func 
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient
from collections import defaultdict
from flask import Flask, jsonify, request
import re
from azure.cosmos import CosmosClient, exceptions
import logging
import pyodbc
import openai
from thefuzz import process, fuzz
import time
from azure.cosmos.exceptions import CosmosHttpResponseError

app = Flask(__name__)

ai_endpoint = os.environ['AI_SERVICE_ENDPOINT']
ai_key = os.environ['AI_SERVICE_KEY']

# Initialize Azure Text Analytics client
credential = AzureKeyCredential(ai_key)
ai_client = TextAnalyticsClient(endpoint=ai_endpoint, credential=credential)

azure_openai_endpoint = "Your_endpoint"
azure_openai_key = "your_key"

# Configure OpenAI to use the Azure endpoint
openai.api_base = azure_openai_endpoint
openai.api_key = azure_openai_key
openai.api_type = 'azure'
openai.api_version = '2024-02-15-preview'


# Connection paraTool2ers
server = os.environ['server']
database = os.environ['database']
username = os.environ['SQLusername']
password = os.environ['password']

database_Tool1 = os.environ['database_Tool1']
connection_string_Tool1 = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"Server=tcp:{server},1433;"
    f"Database={database_Tool1};"
    f"Uid={username};"
    f"Pwd={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=60;"
)
# Tool2rics
Tool2ricsMap = {}
# CosmosDB connection
endpoint = os.environ['dbEndpoint']
key = os.environ['cosmosdbKey']
database_name = os.environ['databaseName']
container_name = os.environ['container']

client = CosmosClient(endpoint, key, timeout=60)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

Prompt_Pattern_Tool2 = """You are an expert in converting English questions to SQL queries. The database you are querying is a Microsoft Azure CosmosDB database with container named 'yours_container'. The data is in JSON format, and specify your primary key .
Also, if there are spelling mistakes, underscores missing, dash missing, or words combined,some part missed then the below key names should be corrected to match those in the items. For example, if the user writes 'dxctechnology8099' or 'dxc Tech', it should be interpreted as 'DXC TECHNOLOGY-8099' or if the user writes 'denmark tax' or 'denmarktax6819', it should be interpreted as 'Denmark Tax-6819' .
If the account name or any other field value has a missing part, such as a first name or number, append the correct value to match the correct item key as well as correct values from the items in the container. Correct spelling mistakes, missing underscores, missing dashes, and combined words.

In CosmosDB each key-value pair is stored in string format even integer value is also in string format for example '2714'.
From the below keys, if any field is provided without using underscores, dashes, abbreviations, or words combined, handle matching keys accordingly.

The container has the following keys:
Mention your all  container key.
The  container has the following keys and 'Values' key having lots of json object is present but there are four key is present for each object and values are also changed for each key:
 give example.
Convert the user's question into an SQL query for  container. Here are some examples of questions and their corresponding SQL queries:
Don't keep duplicate fields in sql query this is mandatory.

1. Question: "Any one question?"
   SQL: Based on the question write query as an example.

Always give result after applying distinct in the query because we don't want duplicates.
If question contains "many" then apply counts in sql query
Furthermore, specific terms should be interpreted as follows:
- 'SubmittedServiceLine' or 'Service Line' or 'Sending Offering' should be interpreted as 'SubmittedServiceLine'.

If the question does not explicitly mention which Items to select, assume the user wants to select all Items. Use 'SELECT * From c' in such cases.
Output should only contain one sql query and nothing else. This is strict requirement.
Now, given the user's question, generate the appropriate SQL query.
"""

Prompt_Pattern_Tool1 = """
You are an expert in converting English questions to SQL queries. The database_Tool1 you are querying is a Microsoft SQL Server database_Tool1 with a table named 'table_name'. This table has the following columns:
Ignore the spelling mistakes and try to match the user input with the field names given below and give the result for the nearest matching column name from below fields.
- RowKey (Primary Key, referred to by users as 'id')
-Your table_ name all column name mention it.

Convert the user's question into an SQL query. Here are some examples of questions and their corresponding SQL queries:

1. Question: "Any one question?"
   SQL: Based on the question write query as an example.

From the above question any question ask take the query as it is. Please be careful while selecting the question and sql query.
Always give result after applying distinct in the query because we don't want duplicates.
If question contains "many" then apply counts in sql query
If the question does not explicitly mention which columns to select, assume the user wants to select all columns. Use 'SELECT *' in such cases.

Also, if there are spelling mistakes, underscores missing, or words combined, the field names should be corrected to match those in the database_Tool1. For example, if the user writes 'plregion' or 'p l region', it should be interpreted as 'P_L_Region'.

Furthermore, specific terms should be interpreted as follows:
- 'fincategory' or 'Approver'  or 'Delievry' or 'AD' should be interpreted as 'fincategory'.

Remember if the column name match is not found from above fields then only check in `demand_state_data` field otherwise give priority to above fields.
The `demand_state_data` column contains nested JSON data. Here is an example of the JSON structure:
Json structure data give example.
In the future, use this context to convert user questions into SQL queries.
"""

def query_openai(prompt):
    try:
        logging.info("Querying OpenAI with the prompt.")
        response = openai.Completion.create(
            engine="gpt_35_turbo",
            prompt=prompt,
            max_tokens=1000,
            stop=["\n", " Answer:"]
        )
        logging.info(f"OpenAI response choices: {response.choices}")
        return response.choices[0].text.strip()
    except Exception as e:
        logging.error(f"Error querying OpenAI: {e}")
        return None

@app.get('/')
def test():    
    return 'Hello, Flask on Azure Functions!cvvvbbb'

def get_openai_response_Tool1(question, Prompt_Pattern):
    startTime = int(time.time())
    prompt = f"{Prompt_Pattern}\n\n{question}"
    response = openai.Completion.create(
        engine="gpt_35_turbo",  # Make sure to use the correct engine name
        prompt=prompt,
        max_tokens=2000,
        temperature=0.5,
    )
    endTime = int(time.time())
    print("Time taken by single get_openai_response_Tool1 in seconds ",endTime -startTime)
    return response.choices[0].text.strip()

def convertUserQuestionToSqlQuery(question,):
    print(f"convertUserQuestionToSqlQuery ::Converting User Question to Sql query: {question}")

    retryCount = 0
    maxRetries = 20
    Transformed_QueryValid = False
 
    while(Transformed_QueryValid == False and retryCount < maxRetries) :
        try:
            retryCount+=1
            print(f"convertUserQuestionToSqlQuery :: retryCount: {retryCount}")

            # cleaning user question
            question = re.sub(r'[^\w\s\'-]+$', '', question).strip()
            question += '?' if not question.endswith('?') else ''
            application = 'Tool1'
            # fetching data from openapi
            Open_API_Response = OpenAIHandle_Resp(question, Prompt_Pattern_Tool1,application)

            if(Open_API_Response == None or Open_API_Response == ''):
                print("convertUserQuestionToSqlQuery :: invalid opeaApiResponse")
                continue

            # sanizing query returned by openapi
            select_index = Open_API_Response.upper().find("SELECT")

            if select_index != -1:
                first_query = Open_API_Response[select_index:]
            else:
                first_query = Open_API_Response
            
            disinfected_query = first_query.split('\n')[0]  # Get the first line after 'SELECT'
            if disinfected_query[-1]=='"':
                disinfected_query =  disinfected_query[:-1]
            disinfected_query = disinfected_query.split(';')[0]
            disinfected_query = disinfected_query.replace("=>", "").replace("`", "").strip()
            print("convertUserQuestionToSqlQuery :: disinfected query: ", disinfected_query)
            
            Transformed_QueryValid = isSqlQueryValid(connection_string_Tool1,disinfected_query)
            
            if(Transformed_QueryValid == True):
                print("convertUserQuestionToSqlQuery :: converted user question to db query", disinfected_query)
                return disinfected_query
            else :
                print("convertUserQuestionToSqlQuery :: invalid conversion of query", disinfected_query)
                
        except exceptions as e:
            print("convertUserQuestionToSqlQuery :: Error in executing Tool2hod",e)

    print("convertUserQuestionToSqlQuery :: could not convert user question to db query", disinfected_query)
    
    return None
 

# Function to retrieve query from the MS SQL database
def read_sql_query(sql, connection_string):
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        logging.info(f"Executing SQL query: {sql}")
        sql = sql.strip('<|im_end|>')
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        conn.commit()
        conn.close()
        return columns, rows
    except pyodbc.Error as e:
        logging.error(f"SQL Error: {e}")
        return [], []

def read_sql_query_Tool1(sql, connection_string):
    print("in read_sql_query")
    print("sql ",sql)
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        logging.info(f"read_sql_query_Tool1 ::Executing SQL query: {sql}")
        if sql == None:
            return [], []
        if '<|im_end|>' in sql:
            sql = sql.strip('<|im_end|>')
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        conn.commit()
        conn.close()
        return columns, rows
    except pyodbc.Error as e:
        logging.error(f"read_sql_query_Tool1 :: SQL Error: {e}")
        return [], []

def isCosmosQueryValid(sql):
    createdQuery = sql + " OFFSET 0 LIMIT 0"
    try:
        items = list(container.query_items(
            query=createdQuery,
            enable_cross_partition_query=True
        ))
        return True
    except exceptions.CosmosHttpResponseError as e:
        print(f"isCosmosQueryValid :: invalid cosmos query  : {sql} , {e}")
        return False

def isSqlQueryValid(connection_string, sql):
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute(sql)
        cursor.fetchone()
        conn.close()
        return True
    except Exception as e:
        print(f"An error occurred: {e}")
        return False


def get_openai_response_Tool2(question, Prompt_Pattern):
    startTime = int(time.time())
    prompt = f"{Prompt_Pattern}\n\n{question}"
    response = openai.Completion.create(
        engine="gpt_35_turbo",  #  use the correct engine name
        prompt=prompt,
        max_tokens=250,
        temperature=0.5,
        top_p = 0.5
    )
    endTime = int(time.time())
    print("Time taken by single get_openai_response_Tool2 in seconds ",endTime -startTime)
    return response.choices[0].text.strip()

def OpenAIHandle_Resp(question,Prompt_Pattern,app):
    print("OpenAIHandle_Resp:: Entering")

    response = None
    startTime = int(time.time())
    
    try:
        if app == 'Tool2':
            response = get_openai_response_Tool2(question, Prompt_Pattern)
        else:
            response = get_openai_response_Tool1(question, Prompt_Pattern)
    except Exception as e:
        print("OpenAIHandle_Resp :: error in calling open api: ", e)
        return None
   
    endTime = int(time.time())

    print("OpenAIHandle_Resp :: Time taken in seconds ", endTime - startTime)

    # print("OpenAIHandle_Resp :: response: ", response)

    return response
 


def read_cosmos_query(sql):
    try:
        print(f"Executing the query: {sql}")
        global container
        items = []
        try:
            items = list(container.query_items(
                query=sql,
                enable_cross_partition_query=True
            ))
        except CosmosHttpResponseError as e:
            print("Error: Data fetching failed in Tool2Container:", e)
            return [], []

        columns = []
        rows = []

        if items:
            try:
                # Attempt to get columns from the first item
                if isinstance(items[0], dict):
                    columns = list(items[0].keys())
                    for item in items:
                        if isinstance(item, dict):
                            row = {key: value for key, value in item.items() if key in columns}
                        else:
                            print(f"Unexpected item format: {item}")
                            # Use the item itself as the key
                            row = {"data": item}
                        rows.append(row)
                else:
                    print("Error: 1st item is not a dictionary.")
                    for item in items:
                        row = {"data": item}
                        rows.append(row)
            except AttributeError as e:
                print("Error: Unexpected item format. Expected dictionary.")
                print(f"Exception: {e}")
                for item in items:
                    row = {"data": item}
                    rows.append(row)

            return columns, rows
        else:
            return [], []

    except CosmosHttpResponseError as e:
        print(f"Cosmos DB Error: {e}")
        return [], []
    except (ValueError, TypeError) as e:
        print(f"Unexpected Error: {e}")
        return [], []


def convertUserQues_To_Db_Query(question,):
    print(f"convertUserQues_To_Db_Query ::Converting User Question to Sql query: {question}")

    retryCount = 0
    maxRetries = 4
    Transformed_QueryValid = False
 
    while(Transformed_QueryValid == False and retryCount < maxRetries) :
        try:
            retryCount+=1
            print(f"convertUserQues_To_Db_Query :: retryCount: {retryCount}")

            # cleaning user question
            question = re.sub(r'[^\w\s\'-]+$', '', question).strip()
            question += '?' if not question.endswith('?') else ''

            # fetching data from openapi
            application = 'Tool2'
            Open_API_Response = OpenAIHandle_Resp(question, Prompt_Pattern_Tool2,application)

            if(Open_API_Response == None or Open_API_Response == ''):
                print("convertUserQues_To_Db_Query :: invalid opeaApiResponse")
                continue

            # sanizing query returned by openapi
            select_index = Open_API_Response.upper().find("SELECT")

            if select_index != -1:
                first_query = Open_API_Response[select_index:]
            else:
                first_query = Open_API_Response
            
            disinfected_query = first_query.split('\n')[0]  # Get the first line after 'SELECT'
            if disinfected_query[-1]=='"':
                disinfected_query =  disinfected_query[:-1]
            
            disinfected_query = disinfected_query.replace("=>", "").replace("`", "").strip()
            print("convertUserQues_To_Db_Query :: disinfected query: ", disinfected_query)
            
            Transformed_QueryValid = isCosmosQueryValid(disinfected_query)

            if(Transformed_QueryValid == True):
                print("convertUserQues_To_Db_Query :: converted user question to db query", disinfected_query)
                return disinfected_query
            else :
                print("convertUserQues_To_Db_Query :: invalid conversion of query", disinfected_query)
                
        except exceptions as e:
            print("convertUserQues_To_Db_Query :: Error in executing Tool2hod",e)

    print("convertUserQues_To_Db_Query :: could not convert user question to db query", disinfected_query)
    
    return None
   
def apiQueryTool2Handler():
    hitTime = int(time.time())
    data = request.json
    user_question = data.get('question')

    if not user_question:
        return jsonify({"error": "Invalid request. 'question' field is required."}), 400

    try:
        db_query = convertUserQues_To_Db_Query(user_question)
        if db_query is None:
            return jsonify({"error": "Could not convert user question to a valid DB query."}), 400

        startTime = int(time.time())
        columns, rows = read_cosmos_query(db_query)
        endTime = int(time.time())
        cosmosFetchTime = endTime - startTime
        Tool2ricsMap["cosmosFetchTime"] = cosmosFetchTime

        print("Time taken by single read_cosmos_query in seconds", cosmosFetchTime)

        if "COUNT" in db_query:
            count = rows[0].get('data', 0) if rows else 0
            human_readable_results = f"Count: {count}"
            return jsonify({
                "results": human_readable_results,
                "summary": f"Total count is {count}."
            })
        else:
            if rows:
                human_readable_results = ", ".join(
                    f"{', '.join(f'{key}: {value}' for key, value in row.items())}" for row in rows
                )
                total = int(time.time()) - hitTime
                return jsonify({
                    "results": human_readable_results.strip(','),
                    "TotalTime": total
                })
            else:
                return jsonify({"results": "No data found."})

    except CosmosHttpResponseError as e:
        print("Cosmos DB Error:", e)
        return jsonify({"error": str(e)}), 500
    except Exception as e:
        print("Failing in api_query_Tool2", e)
        return jsonify({"error": "An unexpected error occurred. Please try again."}), 500

@app.post('/Tool2')   
def apiQueryTool2Controller(): 
    startTimeInSeconds = int(time.time())
    response = apiQueryTool2Handler()
    endTimeInSeconds = int(time.time())
    Tool2ResponseTime = endTimeInSeconds - startTimeInSeconds
    print("Total Response time by /Tool2    : ", Tool2ResponseTime)
    print("Returning response  /Tool2...")
    return response




# Function to format the human-readable results
def formatted_response_prompt_Tool1(human_readable_results):
    prompt = f"""
    Based on the data from the database_Tool1, provide a meaningful one-sentence summary {human_readable_results}
    """
    return prompt

def apiQueryTool1Handler():
    hitTime = int(time.time())
    data = request.json
    user_question = data.get('question')
    try:
        if user_question:
            db_query = convertUserQuestionToSqlQuery(user_question)
            startTime = int(time.time())
            columns, rows = read_sql_query_Tool1(db_query,connection_string_Tool1)
            endTime = int(time.time())
            sqlFetchTime = endTime - startTime
            Tool2ricsMap["sqlFetchTime"] = sqlFetchTime
            
            print("Time taken by single read_sql_query_Tool1 in seconds ",sqlFetchTime)

            try:
                if rows:
                    human_readable_results = ""
                    for row in rows:
                        result = dict(zip(columns, row))
                        formatted_result = ", ".join(f"{key}: {value}" for key, value in result.items())
                        # formatted_result = ", ".join(f"{value}" for value in result.values())
                        human_readable_results += f" {formatted_result},"
                    
                    final_response_prompt = formatted_response_prompt_Tool1(human_readable_results)
                    formatted_response = query_openai(final_response_prompt)
                    total = int(time.time())-hitTime
                    return jsonify({
                        "totalTime": total,
                        "results": human_readable_results.strip().strip(','),
                        "summary": formatted_response
                    })
                else:
                    return return_empty_response()
                    
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        else:
            return jsonify({"error": "Invalid request. 'question' field is required."}), 400
    except exceptions as e:
        print("apiQueryTool1Handler :: Question not found ",e)
        return "please try again"
    

@app.post('/Tool1')   
def apiQueryTool1Controller(): 
    logging.info("Request received /Tool1...")
    startTimeInSeconds = int(time.time())
    response = apiQueryTool1Handler()
    endTimeInSeconds = int(time.time())
    Tool1ResponseTime = endTimeInSeconds - startTimeInSeconds
    print("Total Response time by /Tool1    : ", Tool1ResponseTime)
    print("Returning response  /Tool1...")
    return response
   
def return_empty_response():
    response_data = {
        "results": "",
        "summary": ""
    }
    return jsonify(response_data)

def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    """Each request is redirected to the WSGI handler.
    """
    logging.error("function app entered")
    return func.WsgiMiddleware(app.wsgi_app).handle(req, context)
