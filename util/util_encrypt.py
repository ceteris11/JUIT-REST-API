
import base64
import hashlib
from Crypto import Random
from Crypto.Cipher import AES


class AESCipher:
    def __init__(self, secret_key):
        self.BS = 16
        self.pad = lambda s: s + (self.BS - len(s) % self.BS) * chr(self.BS - len(s) % self.BS)
        self.unpad = lambda s: s[0:-s[-1]]
        self.key = secret_key

    def encrypt(self, raw):
        raw = self.pad(raw).encode('utf-8')
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw))

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        iv = enc[:16]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self.unpad(cipher.decrypt(enc[16:]))

    def encrypt_str(self, raw):
        return self.encrypt(raw).decode('utf-8')

    def decrypt_str(self, enc):
        if type(enc) == str:
            enc = str.encode(enc)
        return self.decrypt(enc).decode('utf-8')


if __name__ == '__main__':
    temp = AESCipher('12345678901234567890123456789012')
    print(temp.encrypt_str('asdfasdfasdfasdf'))
