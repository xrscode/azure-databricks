def update_host_for_token(usr):
    # Access file:
    with open("./src/get_access_key.py", 'r') as file:
        content = file.readlines()
    # Iterate through content file to replace host:
    for i, line in enumerate(content):
        if line[0:12] == 'account_name':
            # Edit the code:
            content[i] = f'account_name = "{usr}"\n'
    # Write new code to file
    with open("./src/get_access_key.py", 'w') as file:
        file.writelines(content)


def update_tfvars():
    # HOST URL:
    print('Please provide host url')
    host = input()
    print(f"'{host}' added, thank you.")

    # TOKEN:
    print('Please provide token')
    token = input()
    print(f"'{token}' provided thank you.")

    # USER:
    print('Please provide user id')
    user = input()
    print(f"'{user}' provided thank you.")

    # Will update get_access_key.py with correct user
    update_host_for_token(user)

    with open('./databricks/terraform.tfvars', 'w') as f:
        f.write(f"""databricks_user = "{user}" \ndatabricks_host = "{host}" \ndatabricks_token = "{token}"
        """)
    return 'tfvars updated.'


update_tfvars()
