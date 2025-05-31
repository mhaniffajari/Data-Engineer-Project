FROM python:3.9

# python dependencies
COPY requirements.txt ./

# Read the requirement
RUN pip install --no-cache-dir -r requirements.txt

# copy folder
WORKDIR /usr/src/app

# copy folder
COPY . .

# export credential
ENV GOOGLE_APPLICATION_CREDENTIALS - "/usr/src/app/firm-pentameter-363006-e587f51df26f.json"

# run the py.file
CMD ["python", "/usr/src/app/main.py"]
