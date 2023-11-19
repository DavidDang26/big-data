

import pandas as pd
import email
from io import StringIO
import boto3


def get_text_from_email(msg):
    '''To get the content from email objects'''
    parts = []
    for part in msg.walk():
        if part.get_content_type() == 'text/plain':
            parts.append(part.get_payload())
    return ''.join(parts)


def split_email_addresses(line):
    '''To separate multiple email addresses'''
    if line:
        addrs = line.split(',')
        addrs = frozenset(map(lambda x: x.strip(), addrs))
    else:
        addrs = None
    return addrs


def get_csv_url():
    bucket = 'prj-datdang'  # already created on S3
    file_name = "emails.csv"

    s3 = boto3.client('s3')
    # 's3' is a key word. create connection to S3 using default config and all buckets within S3

    obj = s3.get_object(Bucket=bucket, Key=file_name)
    print("obj", obj)
    # get object and file (key) from bucket

    emails_df = pd.read_csv(obj['Body'])  # 'Body' is a key word

    # Change data URL based on actual location
    # emails_df = pd.read_csv("emails.csv")
    print(emails_df.shape)
    emails_df.head()

    print(emails_df['message'][0])

    # work with a small selection
    s_email_df = emails_df.sample(100_000)

    # Extract data from message

    messages = list(map(email.message_from_string, s_email_df['message']))
    s_email_df.drop('message', axis=1, inplace=True)
    # Get fields from parsed email objects
    keys = messages[0].keys()
    for key in keys:
        s_email_df[key] = [doc[key] for doc in messages]
    # Parse content from emails
    s_email_df['content'] = list(map(get_text_from_email, messages))
    # Split multiple email addresses
    s_email_df['From'] = s_email_df['From'].map(split_email_addresses)
    s_email_df['To'] = s_email_df['To'].map(split_email_addresses)

    # Extract the root of 'file' as 'user'
    s_email_df['user'] = s_email_df['file'].map(lambda x: x.split('/')[0])
    del messages

    s_email_df.head()

    # selection
    # Get the top 100 frequent receiver
    to_selection = s_email_df['X-To'].value_counts()[0:100].index
    # Get the top 100 frequent sender
    from_selection = s_email_df['X-From'].value_counts()[0:100].index

    # Get emails whom sender is among the top 100 frequent sender AND receiver is among the top 100 frequent receiver
    sc_email_df = s_email_df.loc[s_email_df['X-To'].isin(
        to_selection) & s_email_df['X-From'].isin(from_selection)]
    sc_email_df.shape

    sc_email_df = sc_email_df[['To', 'From', 'X-To', 'X-From', 'content']]

    sc_email_df.head(20)

    # Remove emails which X-To contains @ or X-From contains @
    m_email_df = sc_email_df.loc[~sc_email_df["X-To"].str.contains('@')]
    m_email_df = m_email_df.loc[~m_email_df["X-From"].str.contains('@')]

    # Remove emails which X-To contains < or X-From contains <
    m_email_df = m_email_df.loc[~m_email_df["X-From"].str.contains('<')]
    m_email_df = m_email_df.loc[~m_email_df["X-To"].str.contains('<')]

    # Remove emails which X-To contains ,
    m_email_df = m_email_df.loc[~m_email_df["X-To"].str.contains(',')]
    # Remove emails which X-To is empty
    m_email_df = m_email_df.loc[~m_email_df["To"].isna()]

    m_email_df.shape

    m_email_df.head()

    # Add "to_index" column
    m_email_df["to_index"] = "From " + m_email_df["X-From"] + \
        " to " + m_email_df["X-To"] + ": " + m_email_df["content"]
    m_email_df.head()

    # Output to csv file after selection
    # m_email_df.to_csv("proc_email.csv", index=False)
    csv_buffer = StringIO()
    m_email_df.to_csv(csv_buffer)

    s3_resource = boto3.resource('s3')

    s3_resource.Object(bucket, 'proc_email.csv').put(
        Body=csv_buffer.getvalue())
    url = s3.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket,
            'Key': 'proc_email.csv'
        },
        ExpiresIn=3600  # one hour in seconds, increase if needed
    )
    return url
