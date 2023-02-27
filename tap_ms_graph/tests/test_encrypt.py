import json
from tap_ms_graph.tap import TapMSGraph
from tap_ms_graph.encrypt import decrypt

ROW = {"id": "AAMkAGI4YzNhZmYwLTVhNTEtNGNmNi04OTAwLWQzMTBkNDg0ZmI3MABGAAAAAADjepwEQLvQSYUYL6eraVTJBwBRTI2xxc1ZRL1BxUhPrNHEAAAAAAEMAABRTI2xxc1ZRL1BxUhPrNHEAAAu1o8VAAA=", "sentDateTime": "2021-08-27T05:42:12Z", "internetMessageId": "<MWHPR1201MB02078FCA2832C700ED74F346F4C89@MWHPR1201MB0207.namprd12.prod.outlook.com>", "conversationId": "AAQkAGI4YzNhZmYwLTVhNTEtNGNmNi04OTAwLWQzMTBkNDg0ZmI3MAAQALhSwxItz81Lm2iv_at2R5A=", "sender": {"emailAddress": {"name": "MyAnalytics", "address": "no-reply@microsoft.com"}}, "from": {"emailAddress": {"name": "MyAnalytics", "address": "no-reply@microsoft.com"}}, "toRecipients": [{"emailAddress": {"name": "Anna Smith", "address": "A.Smith@nexient.com"}}], "ccRecipients": [], "bccRecipients": [], "replyTo": []}
with open(".secrets/config.json", "r") as f:
    CONFIG = json.loads(f.read())

def test_encrypt():
    email =  ROW['toRecipients'][0]['emailAddress']['address']
    CONFIG['hash_email'] = True
    CONFIG['encrypt_email'] = True
    tap = TapMSGraph(config=CONFIG)
    streams = tap.discover_streams()
    messages_stream = streams[2]
    messages_stream.post_process(ROW, None)
    encrypted_message = ROW['toRecipients'][0]['emailAddress']['encryptedAdress']
    encrypted_message = bytes(encrypted_message, "utf-8")
    assert email.lower() == decrypt(encrypted_message)