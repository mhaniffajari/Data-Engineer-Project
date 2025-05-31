import streamlit as st
import gnupg
import os

# Initialize GPG
gpg = gnupg.GPG()

# Function to generate and export a new GPG key
def generate_key():
    input_data = gpg.gen_key_input(name_email="user@example.com", passphrase="p@ssphr@se167?")
    key = gpg.gen_key(input_data)
    
    # Export the private and public keys
    public_key = gpg.export_keys(key.fingerprint)
    private_key = gpg.export_keys(key.fingerprint, secret=True, passphrase="p@ssphr@se167?")
    
    # Save both keys to files
    with open('public_key.asc', 'w') as pub_file:
        pub_file.write(public_key)
    with open('private_key.asc', 'w') as priv_file:
        priv_file.write(private_key)

    return public_key, private_key

# Function to encrypt a file with GPG
def encrypt_file(file_data, public_key):
    # Import the public key into the GPG keyring
    import_result = gpg.import_keys(public_key)
    
    # Ensure at least one key is imported
    if import_result.count == 0:
        raise ValueError("No valid GPG keys were imported.")
    
    # Use the fingerprint of the imported public key as the recipient
    recipient_fingerprint = import_result.fingerprints[0]
    
    # Encrypt the file using the recipient's public key
    encrypted_data = gpg.encrypt(file_data, recipients=[recipient_fingerprint], always_trust=True, armor=False)
    
    if not encrypted_data.ok:
        raise ValueError(f"Encryption failed: {encrypted_data.status}")
    
    return str(encrypted_data)

# Function to decrypt a file with GPG
def decrypt_file(file_data, private_key, passphrase):
    decrypted_data = gpg.decrypt(file_data, passphrase=passphrase)
    return str(decrypted_data)

# Streamlit UI
st.title("File Encryption and Decryption with GPG")

# Key generation section
st.header("Generate Key")
if "key_generated" not in st.session_state:
    st.session_state.key_generated = False

if not st.session_state.key_generated:
    if st.button("Generate New GPG Key"):
        public_key, private_key = generate_key()
        st.session_state.public_key = public_key
        st.session_state.private_key = private_key
        st.session_state.key_generated = True
        st.success("New GPG key pair generated")
else:
    st.write("GPG keys already generated.")

# Display download buttons for keys if they are generated
if st.session_state.key_generated:
    st.download_button(
        label="Download Public Key",
        data=st.session_state.public_key,
        file_name="public_key.asc",
        mime="application/pgp-keys"
    )
    
    st.download_button(
        label="Download Private Key",
        data=st.session_state.private_key,
        file_name="private_key.asc",
        mime="application/pgp-keys"
    )

# Encrypt file section
st.header("Encrypt a File")
uploaded_file_encrypt = st.file_uploader("Choose a file to encrypt", type=["csv", "txt"])
if uploaded_file_encrypt is not None:
    key_file = st.file_uploader("Upload the recipient's public key", type=["asc"])
    if key_file is not None:
        public_key = key_file.read().decode()
        file_data = uploaded_file_encrypt.read()
        encrypted_data = encrypt_file(file_data, public_key)
        
        st.download_button(
            label="Download Encrypted File",
            data=encrypted_data,
            file_name=f"{uploaded_file_encrypt.name}.gpg",
            mime="application/octet-stream"
        )
        st.success("File encrypted successfully!")

# Decrypt file section
st.header("Decrypt a File")
uploaded_file_decrypt = st.file_uploader("Choose a file to decrypt", type=["gpg"])
if uploaded_file_decrypt is not None:
    key_file = st.file_uploader("Upload your private key", type=["asc"])
    if key_file is not None:
        private_key = key_file.read().decode()
        passphrase = st.text_input("Enter the passphrase", type="password")
        
        if passphrase:
            file_data = uploaded_file_decrypt.read().decode()
            try:
                decrypted_data = decrypt_file(file_data, private_key, passphrase)
                st.download_button(
                    label="Download Decrypted File",
                    data=decrypted_data,
                    file_name=uploaded_file_decrypt.name.replace(".gpg", ""),
                    mime="text/csv"
                )
                st.success("File decrypted successfully!")
            except Exception as e:
                st.error(f"Decryption failed: {e}")
