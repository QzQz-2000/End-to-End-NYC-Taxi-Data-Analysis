# Google cloud platform environment setup

1. Create a [Google Cloud Platform](https://cloud.google.com/) account. You should receive $300 in credit when signing up on GCP for the first time with an account.

2. Set up a new project and write down the Project ID. Make sure that your project is selected. Click on the previous drop down menu to select it otherwise.

3. Set up a *Service Account* for this project. The service account should have the following roles:

   - `BigQuery Admin`

   - `Storage Admin`

   - `Storage Object Admin`

   - `Viewer`

   Add a new key and download the JSON credentials file, rename it to `google_credential.json` and store it in your home directory (`$HOME/.google/credentials/`).

   ```bash
   cd ~ && mkdir -p ~/.google/credentials
   mv <path/to/your/service-account-authkeys>.json ~/.google/credentials/google_credentials.json
   ```

4. Activate the following APIs:

   - https://console.cloud.google.com/apis/library/iam.googleapis.com
   - https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com

5. Install and setup Google Cloud SDK. Download Gcloud SDK [from this link](https://cloud.google.com/sdk/docs/install) and install it according to the instructions for your OS. 

   For mac can simple run:

   ```bash
   brew install --cask google-cloud-sdk
   ```

   Initialize the SDK [following these instructions](https://cloud.google.com/sdk/docs/quickstart).

   - Run `gcloud init` from a terminal and follow the instructions.

   - Make sure that your project is selected with the command `gcloud config list`

6. Create an environment variable for the credentials. Create an environment variable called `GOOGLE_APPLICATION_CREDENTIALS` and assign it to the path of your JSON credentials file, which would be `$HOME/.google/credentials/`.  Assuming you're running bash:

   ```bash
   vim ~/.bashrc
   
   # at the end of the file, add the following line
   export GOOGLE_APPLICATION_CREDENTIALS="<path/to/authkeys>.json"
   
   # save the changes and exit, activate the env variable
   source ~/.bashrc
   ```

7. Refresh the token and verify the authentication with the GCP SDK:

   ```bash
   gcloud auth application-default login
   ```

   