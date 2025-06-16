"""
 * FILENAME: [aws_s3_functionality.py]
 * AUTHOR: [Jeremy Underwood]
 * COURSE: [CYOP 400]
 * PROFESSOR: [Craig Poma]
 * CREATEDATE: [04FEB25]
"""

from __future__ import print_function
import json
import sys
import boto3
from boto3.dynamodb.conditions import Key, And

# Initialize a DynamoDB resource object
dynamodb = boto3.resource('dynamodb')
# Reference the 'Courses' table in DynamoDB
table = dynamodb.Table('Courses')

# Create the Courses table
def create_table():
    """
    Create a DynamoDB table named 'Courses' with specified key schema and attributes.

    This function creates the 'Courses' table with the following schema:
    - Primary Key: CourseID (String)

    The function also waits until the table exists before proceeding.

    The code to create the table is based on the UsingDynamoDB PDF from
    the course learning resources

    The table.wait_until_exists() statement comes from the Boto3 documentation
    at the following URL: https://boto3.amazonaws.com/v1/documentation/api/latest/
    reference/services/dynamodb/table/wait_until_exists.html
    """
    try:
        # Create the Courses table and schema
        table = dynamodb.create_table(
            TableName='Courses',
            KeySchema=[
                {
                    'AttributeName': 'CourseID',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'CourseID',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
            }
        )
        # Wait until the table exists before continuing
        table.wait_until_exists()
        print("Table created!")
    except Exception as e:
        print("Table already exists")

def enter_table_items():
    """
    Enter course items into the 'Courses' DynamoDB table from a JSON file.

    This function reads course data from the 'course_items.json' file and inserts each course
    into the 'Courses' table.

    This function is based on the MoviesLoadData.py file from the DynamoDB-PythonCode ZIP
    """
    try:
        # Open the JSON file and load course data
        with open("homework_2/course_items.json") as json_file:
            courses = json.load(json_file)
            for course in courses:
                course_id = course['CourseID']
                subject = course['Subject']
                catalog_number = course['CatalogNbr']
                title = course['Title']
                credits = course['NumCredits']

                print(f"Adding course: {subject}{catalog_number}")
                # Put items in Courses table
                table.put_item(
                    Item={
                        "CourseID": course_id,
                        "Subject": subject,
                        "CatalogNbr": catalog_number,
                        "Title": title,
                        "NumCredits": credits
                    }
                )
    except Exception as e:
        print("Issue loading items!")
        return None


def get_course_title(subject, catalog_nbr):
    """
    Retrieve the course title based on the given subject and catalog number.

    This function queries the 'Courses' table to find the course with the specified subject
    and catalog number and returns its title.

    Table query was extracted from the code provided in the MoviesQuery02.py from
       DynamoDB-PythonCode.zip

    The scan is from Professor Poma's reply to Mariam Ahmed's post "Question about homework 4"
    """
    filters = {
        'Subject': subject,
        'CatalogNbr': catalog_nbr
    }
    try:
        # Scan the 'Courses' table using a filter expression that checks if the attributes in the 'filters' dictionary
        # match the specified values. The filter expression is constructed by combining conditions using 'And',
        # where each condition checks for equality between a key and its corresponding value.
        response = table.scan(
            FilterExpression=And(*[(Key(key).eq(value)) for key, value in filters.items()])
        )
    except Exception as e:
        return None

    # Check if the 'Items' key exists in the response and if the list of items is not empty.
    # If there are items, return the 'Title' of the first item.
    # If no items are found, return None.
    if 'Items' in response and len(response['Items']) > 0:
        return response['Items'][0]['Title']
    else:
        return None


# Run the interface in a loop to keep the program running
def prompt_user():
    """
    Run the user interface for the Catalog Search program.

    This function prompts the user to enter a course subject and catalog number to search for
    the course title in the 'Courses' table. It continues to run in a loop until the user
    chooses to exit.
    """
    while True:
        print("\nWelcome to the Catalog Search program!")
        subject = input('Enter the Subject:')
        catalog_nbr = input('Enter the CatalogNbr:')
        if not subject or not catalog_nbr:
            print("Both a subject and a catalog number are required!")
            continue
        course_title = get_course_title(subject, catalog_nbr)
        if course_title:
            print(f"The title of {subject} {catalog_nbr} is {course_title}")
        else:
            print(f"{subject} {catalog_nbr} not found!")
        choice = input("Would you like to search for another title? (Y or N)")
        if choice.lower() not in ("n", "y"):
            print("Invalid input!")
        else :
            if choice.lower() == "n":
                print("Thanks for using the Catalog Search program.")
                sys.exit("Program Terminating")
            else:
                continue


def main():
    """
    The main function calls the functions to create a table,
    enter the table items, and then prompt the user.
    """
    create_table()
    enter_table_items()
    prompt_user()


main()
