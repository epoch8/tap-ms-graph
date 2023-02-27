import hashlib
from urllib.parse import urlparse
from .encrypt import encrypt
from singer_sdk.streams import Stream as RESTStreamBase

EMAIL_FIELDS = ["attendees", "toRecipients", "ccRecipients", "bccRecipients", "from", "sender", "organizer"]


class MSGraphUtils:
    def __init__(self, stream: RESTStreamBase) -> None:
        self.stream = stream

    def hash_email_in_row(self, row):
        for email_object_name in EMAIL_FIELDS:
            if row.get(email_object_name):
                email_object = row.pop(email_object_name)
                if isinstance(email_object, dict):
                    email_object = self.hash_email(email_object)
                elif isinstance(email_object, list):
                    email_object = self.hash_email_in_array(email_object)
                row.update({email_object_name: email_object})
        return row


    def hash_email_in_array(self, email_objects_array):
        for email_object in email_objects_array:
            email_object = self.hash_email(email_object)
        return email_objects_array

    def hash_email(self, email_object):
        if email_object["emailAddress"].get("address"):
            if self.stream.config.get("encrypt_email"):
                email_object["emailAddress"]["encryptedAdress"] = self.encrypt_email(email_object["emailAddress"]["address"].lower())
            email_object["emailAddress"]["address"] = self.md5(email_object["emailAddress"]["address"].lower())
        else:
            email_object["emailAddress"]["address"] = ""
        email_object["emailAddress"].pop("name")
        return email_object

    def encrypt_email(self, address):
        address = bytes(address, "utf-8")
        encrypted = encrypt(address, self.stream.config["public_key_path"])
        encrypted = str(encrypted, encoding="utf-8")
        return encrypted
        
    @staticmethod
    def md5(input: str) -> str:
        return hashlib.md5(input.encode("utf-8")).hexdigest()
    
    @staticmethod
    def get_domain_name_from_url_in_row(row):
        if row.get("onlineMeeting"):
            row["onlineMeeting"]["joinUrl"] = urlparse(row["onlineMeeting"]["joinUrl"]).hostname
        return row

    @staticmethod
    def filter_message_headers(row):
        if not row.get("internetMessageHeaders"):
            return row
        headers = row.pop("internetMessageHeaders")
        headers = (filter(lambda x: x['name'] == 'In-Reply-To', headers))
        headers = list(map(lambda x: {'name': x['name'].lower(), 'value': x['value']}, headers))
        row.update({"internetMessageHeaders": headers})
        return row 