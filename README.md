# How to run this docker

1. Open your Docker and Visual Studio Code and navigate to the terminal.
2. Clone the repository by typing the following command and pressing Enter:
   ```
   git clone https://github.com/mhaniffajari/TIA_ETL.git
   ```
3. Navigate to the cloned directory by typing:
   ```
   cd TIA_ETL
   ```
4. Build the Docker image by typing the following command and pressing Enter:
   ```
   docker build -t my-airflow .
   ```
5. Start the containers using Docker Compose by typing:
   ```
   docker-compose up -d
   ```
6. Wait for the containers to run successfully.
7. Once the containers are running, you can check the DAGs by visiting [http://localhost:8080/](http://localhost:8080/) in your web browser.
8. Use the following credentials to log in:
   - Username: airflow
   - Password: airflow
9. You can then run the DAGs named "page_techinasia_pipeline" and "comments_techinasia_pipeline" from the Airflow UI.
