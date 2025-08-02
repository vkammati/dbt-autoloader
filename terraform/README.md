# Terraform Project

[Azure Terraform Documentation](https://developer.hashicorp.com/terraform/tutorials/azure-get-started/azure-build)


Basic commands:
- Get started!
    ```
    terraform init
    ```
- Format configuration
    ```
    terraform fmt
    ```
- Validate configuration
    ```
    terraform validate
    ```
- Return the diff between code and actual resources
    ```
    terraform plan
    ```
- Create, update or destroy resources
    ```
    terraform apply
    ```
  If you like to save the log in a file
    ```
    terraform apply -auto-approve > "../../scratch/tf_apply_output_$(date +%Y%m%d_%H%M%S).txt" 2>&1
    ```
- Inspect the current state. Which
    ```
    terraform show
    ```
- Destroy all resources
    ```
    terraform destroy
    ```
- To destroy and recreate a resource
    ```
    terraform taint <resource_type.resource_name>
    terraform apply
    ```
