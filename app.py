import streamlit as st
from cryptography.fernet import Fernet
import os

# Function to generate and save a key
def generate_key():
    key = Fernet.generate_key()
    with open('key.key', 'wb') as key_file:
        key_file.write(key)
    return key

# Function to load the key from a file
def load_key(key_file):
    if key_file:
        return key_file.read()
    else:
        st.error("Please upload a valid key file.")
        return None

# Function to encrypt the file
def encrypt_file(file_data, key):
    fernet = Fernet(key)
    encrypted_data = fernet.encrypt(file_data)
    return encrypted_data

# Function to decrypt the file
def decrypt_file(file_data, key):
    fernet = Fernet(key)
    decrypted_data = fernet.decrypt(file_data)
    return decrypted_data

# Streamlit UI
st.title("File Encryption and Decryption")

# Key generation section
st.header("Generate Key")
if st.button("Generate New Key"):
    key = generate_key()
    st.success("New key generated and saved to 'key.key'")
    
    # Option to download the key
    st.download_button(
        label="Download Key",
        data=key,
        file_name="key.key",
        mime="application/octet-stream"
    )

# Encrypt file section
st.header("Encrypt a File")
uploaded_file_encrypt = st.file_uploader("Choose a file to encrypt", type=["csv", "txt"])
if uploaded_file_encrypt is not None:
    key_file = st.file_uploader("Upload your key to encrypt", type=["key"])
    if key_file is not None:
        key = load_key(key_file)
        if key:
            file_data = uploaded_file_encrypt.read()
            encrypted_data = encrypt_file(file_data, key)
            st.download_button(
                label="Download Encrypted File",
                data=encrypted_data,
                file_name=f"{uploaded_file_encrypt.name}.encrypted",
                mime="application/octet-stream"
            )
            st.success("File encrypted successfully!")

# Decrypt file section
st.header("Decrypt a File")
uploaded_file_decrypt = st.file_uploader("Choose a file to decrypt", type=["encrypted"])
if uploaded_file_decrypt is not None:
    key_file = st.file_uploader("Upload your key to decrypt", type=["key"])
    if key_file is not None:
        key = load_key(key_file)
        if key:
            file_data = uploaded_file_decrypt.read()
            try:
                decrypted_data = decrypt_file(file_data, key)
                st.download_button(
                    label="Download Decrypted File",
                    data=decrypted_data,
                    file_name=uploaded_file_decrypt.name.replace(".encrypted", ""),
                    mime="text/csv"
                )
                st.success("File decrypted successfully!")
            except Exception as e:
                st.error(f"Decryption failed: {e}")
