import hashlib
from urllib.parse import urlparse

EMAIL_OBJECTS_ARRAYS = ["attendees", "toRecipients", "ccRecipients", "bccRecipients"]
EMAIL_OBJECTS = ["from", "sender", "organizer"]


def get_domain_name_from_url_in_row(row):
    if row.get("onlineMeeting"):
        row["onlineMeeting"]["joinUrl"] = urlparse(row["onlineMeeting"]["joinUrl"]).hostname
    return row

def filter_message_headers(row):
    if not row.get("internetMessageHeaders"):
        return row
    headers = row.pop("internetMessageHeaders")
    headers = (filter(lambda x: x['name'] == 'In-Reply-To', headers))
    headers = list(map(lambda x: {'name': x['name'].lower(), 'value': x['value']}, headers))
    row.update({"internetMessageHeaders": headers})
    return row 

def hash_email_in_email_objects(row):
    for email_object_name in EMAIL_OBJECTS:
        if row.get(email_object_name):
            email_object = row.pop(email_object_name)
            email_object = hash_email(email_object)
            row.update({email_object_name: email_object})
    return row

def hash_email_in_email_objects_array(row):
    for email_objects_array in EMAIL_OBJECTS_ARRAYS:
        if row.get(email_objects_array):
            email_objects = row.pop(email_objects_array)
            email_objects = hash_email_in_array(email_objects)
            row.update({email_objects_array: email_objects})
    return row

def hash_email_in_array(email_objects_array):
    for email_object in email_objects_array:
        email_object = hash_email(email_object)
    return email_objects_array

def hash_email(email_object):
    if email_object["emailAddress"].get("address"):
        email_object["emailAddress"]["address"] = md5(email_object["emailAddress"]["address"].lower())
    else:
        email_object["emailAddress"]["address"] = ""
    email_object["emailAddress"].pop("name")
    return email_object

def md5(input: str) -> str:
    return hashlib.md5(input.encode("utf-8")).hexdigest()