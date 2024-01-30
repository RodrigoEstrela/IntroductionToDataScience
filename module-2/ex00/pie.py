import psycopg2
import matplotlib.pyplot as plt
import pandas as pd


def american_pie(data):
    """
    Displays a pie chart of the values in 'event_type' column
    """
    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data, columns=['event_type'])

    # Count the occurences of each event type
    event_counts = df['event_type'].value_counts()

    # Create a pie chart with the count of each event type
    plt.pie(event_counts, labels=event_counts.index, autopct='%1.1f%%')
    plt.title('Event Types')
    plt.show()


def main():
    """
    Establishes the db connection
    Gets the data from the table
    Closes the connection
    Sends the data to the function to display the pie chart
    """

    # Connection parameters
    db_params = {
        'host': 'localhost',
        'port': '5432',
        'database': 'piscineds',
        'user': 'rdas-nev',
        'password': 'mysecretpassword'
    }

    # Establish a connection
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Get the data from the db
    cur.execute("SELECT event_type FROM customers")
    data = cur.fetchall()

    # Close the connection
    cur.close()
    conn.close()

    # Call the function
    american_pie(data)


if __name__ == '__main__':
    main()
