import subprocess
import os
import json


def check_azure_login():
    try:    
        result = subprocess.run(["az", "login"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        login_info = json.loads(result.stdout)
        user_name = login_info[0]['user']['name']
        check_file_exists(user_name)
    except Exception as e:
        return f"Unable to login.  Error: {e}"

        


def check_file_exists(user):
    """
    This function will check to see if 'terraform.tfvars' exists. 
    If it does not exist, it will create the file and write the username to a variable.

    Args: username.

    Returns: True if file updated or created successfully.
    Error message if file not updated or not created successfully.
    """
    check_file = os.path.isfile("terraform/terraform.tfvars")

    if check_file:
        try:
            with open("terraform/terraform.tfvars", "w") as tf_file:
                tf_file.write(f'databricks_user        = "{user}"')   
                return True       
        except Exception as e:
            return f"Unable to write to file, error message: {e}."
    else:
        try:
            with open("terraform/terraform.tfvars", "x") as tf_file:
                tf_file.write(f'databricks_user        = "{user}"')
                return True
        except Exception as e:
            return f"Unable to create file, error message: {e}."
        

check_azure_login()
