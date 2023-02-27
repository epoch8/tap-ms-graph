from nacl.public import PrivateKey, SealedBox, PublicKey
from nacl.encoding import Base64Encoder

def generate_secret_key(path="./secret_key"):
    secret_key = PrivateKey.generate()
    with open(path, "wb") as f:
        f.write(bytes(secret_key))

def generate_pulic_key(path="./public_key", secret_key_path="./secret_key"):
    with open(secret_key_path, "rb") as f:
        secret_key = PrivateKey(f.read())

    public_key = secret_key.public_key

    with open(path, "wb") as f:
        f.write(bytes(public_key))

def encrypt(message, public_key_path="./public_key"):
    with open(public_key_path, "rb") as f:
        public_key = PublicKey(f.read())
    sealed_box = SealedBox(public_key)
    encrypted = sealed_box.encrypt(message, Base64Encoder)
    return encrypted

def decrypt(ecnrypted_message, secret_key_path="./secret_key"):
    with open(secret_key_path, "rb") as f:
        secret_key = PrivateKey(f.read())

    unseal_box = SealedBox(secret_key)
    plaintext = unseal_box.decrypt(ecnrypted_message, Base64Encoder)
    return plaintext.decode('utf-8')