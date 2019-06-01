#usr/bin/python3
#Icepyck interface for Amazon Glacier


from Crypto import Random
from Crypto.Cipher import AES
from base64 import b64encode, b64decode
from Crypto.Protocol.KDF import PBKDF2

class AESCipher:

    '''A class representing an AES-256 encryption, requires a passphrase argument
    bytestrings can be encrypted or decrypted as arguments to encrypt, decrypt methods'''

    def __init__(self, pphrase):
        self.key = PBKDF2(pphrase, salt=b'djicne3u')
        self.bytesize = 32

    def encrypt(self, raw):
        raw = self._pad(raw)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return b64encode(iv+cipher.encrypt(raw))

    def decrypt(self, enc):
        enc = b64decode(enc)
        iv = enc[:AES.block_size]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')

    @staticmethod
    def _pad(self, s):
        return s + (self.bytesize - len(s) % self.bytesize) * chr(self.bytesize - len(s) % self.bytesize)

    @staticmethod
    def _unpad(self ,s):
        return s[:-ord(s[len(s)-1:])]

this = AESCipher('poop')

mess = 'this is a message to say YEAH'

dave = this.encrypt(mess)

print(mess)
print(dave)
print(this.decrypt(dave))
print(this.key)

file = open('degroot-schervish-2012.pdf', 'rb')
encrypted = open('encrypted_degroot', 'wb')
encrypted.write(dave)
file.close()
encrypted.close()
dfile = open('encrypted_degroot', 'rb')
dcrypt = this.decrypt(dfile)
lasttest = open('decrypted_degroot', 'wb')
lasttest.write(dcrypt)
print(dcrypt)



