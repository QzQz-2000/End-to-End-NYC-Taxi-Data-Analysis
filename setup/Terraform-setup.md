# Terraform setup

1. Navigate to the `terraform` folder.

2. Open `variables.tf` and edit the variable `region` block so that it matches your preferred region.

3. Initiate Terraform and download the required dependencies:

   ```bash
   terraform init
   ```

4. View the Terraform plan and make sure that you are creating the following resources

   - a Google Cloud Storage bucket
   - a BigQuery dataset

   ```bash
   terraform plan
   ```

5. If the plan details are as expected, apply the changes:

   ```bash
   terraform apply
   ```

6. One you are done with the Project, tear down the infra useing:

   ```bash
   terraform destroy
   ```

You should now have a bucket called `nyc-taxi-storage-bucket` and a dataset called `nyc_taxi_dataset` in BigQuery.