import os
from cryptography.fernet import Fernet
import subprocess

class CDPCredentialsHandler:
    CDP_CRED_PATH = r"C:\Users\vutuk\.cdp\credentials"

    def __init__(self, enc_path, key_path, cdp_cred_path=CDP_CRED_PATH):
        self.enc_path = enc_path
        self.key_path = key_path
        self.cdp_cred_path = cdp_cred_path
        self.decrypted_content = None

    def encrypt_if_needed(self):
        if os.path.exists(self.enc_path) and os.path.exists(self.key_path):
            print(" Encrypted credentials already exist. Skipping encryption.")
            return

        if os.path.exists(self.cdp_cred_path):
            print(" Encrypting credentials...")

            try:
                key = Fernet.generate_key()
                with open(self.key_path, 'wb') as f:
                    f.write(key)

                with open(self.cdp_cred_path, 'rb') as f:
                    data = f.read()

                fernet = Fernet(key)
                encrypted = fernet.encrypt(data)

                with open(self.enc_path, 'wb') as f:
                    f.write(encrypted)

                print(" Encryption complete. Encrypted file and key generated.")
                os.remove(self.cdp_cred_path)
                print(" Original credentials file removed.")

            except Exception as e:
                print(" Error during encryption:", str(e))

        else:
            print(" No plaintext credentials file found. Nothing to encrypt.")

    def decrypt_and_prepare_credentials(self):
        if not os.path.exists(self.enc_path) or not os.path.exists(self.key_path):
            raise FileNotFoundError(" Cannot decrypt: Encrypted file or key not found.")

        with open(self.key_path, 'rb') as f:
            key = f.read()

        with open(self.enc_path, 'rb') as f:
            encrypted = f.read()

        fernet = Fernet(key)
        decrypted = fernet.decrypt(encrypted)
        self.decrypted_content = decrypted.decode()

        os.makedirs(os.path.dirname(self.cdp_cred_path), exist_ok=True)
        with open(self.cdp_cred_path, 'w') as f:
            f.write(self.decrypted_content)

    def cleanup_credentials_file(self):
        if os.path.exists(self.cdp_cred_path):
            os.remove(self.cdp_cred_path)

    def run_cdp_command(self, command_args):
        return subprocess.run(command_args, capture_output=True, text=True)