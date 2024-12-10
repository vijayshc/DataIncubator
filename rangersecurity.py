import requests
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
import json
import configparser
import sys

# --- Configuration ---
config = configparser.ConfigParser()
config.read('ranger_config.ini')  # Assuming configuration file is named ranger_config.ini

ranger_host = config['ranger']['host']
ranger_port = config['ranger']['port']
ranger_service_name = config['ranger']['service_name']
ranger_principal = config['ranger']['principal']
ranger_keytab = config['ranger']['keytab']
policy_file = config['general']['policy_file']

# --- Kerberos Authentication ---
# This requires the requests-kerberos and kerberos/gssapi libraries
kerberos_auth = HTTPKerberosAuth(principal=ranger_principal, force_preemptive=True)

# --- Ranger API Base URL ---
ranger_base_url = f"http://{ranger_host}:{ranger_port}/service/public/v2/api"

# --- Helper Functions ---

def get_ranger_policy(policy_name):
    """
    Fetches a Ranger policy by name.

    Args:
        policy_name: The name of the policy to fetch.

    Returns:
        The policy as a JSON object, or None if not found.
    """
    url = f"{ranger_base_url}/policy?policyName={policy_name}"
    try:
        response = requests.get(url, auth=kerberos_auth, verify=False)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        policies = response.json()
        if policies and len(policies) > 0 and policies[0]['name'] == policy_name :
          return policies[0]
        else:
          return None
        
    except requests.exceptions.RequestException as e:
        print(f"Error getting policy: {e}")
        return None

def create_ranger_policy(policy_data):
    """
    Creates a new Ranger policy.

    Args:
        policy_data: The policy data as a JSON object.

    Returns:
        The created policy as a JSON object, or None if creation failed.
    """
    url = f"{ranger_base_url}/policy"
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(url, auth=kerberos_auth, headers=headers, data=json.dumps(policy_data), verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error creating policy: {e}")
        return None

def update_ranger_policy(policy_id, policy_data):
    """
    Updates an existing Ranger policy.

    Args:
        policy_id: The ID of the policy to update.
        policy_data: The updated policy data as a JSON object.

    Returns:
        The updated policy as a JSON object, or None if update failed.
    """
    url = f"{ranger_base_url}/policy/{policy_id}"
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.put(url, auth=kerberos_auth, headers=headers, data=json.dumps(policy_data), verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error updating policy: {e}")
        return None

def read_policy_file(file_path):
  """
  Reads and parses the policy file.

  Args:
      file_path: Path to the policy file.

  Returns:
      A list of dictionaries, where each dictionary represents a policy
      definition with keys: 'database', 'table', 'column', 'masking_option'.
      Returns None if there is an error parsing the file.
  """
  try:
    with open(file_path, 'r') as f:
      policies = []
      for line in f:
        line = line.strip()
        if not line or line.startswith('#'):  # Skip empty lines and comments
          continue

        parts = line.split(',')
        if len(parts) != 4:
          raise ValueError(f"Invalid policy format in line: {line}")

        policies.append({
            'database': parts[0].strip(),
            'table': parts[1].strip(),
            'column': parts[2].strip(),
            'masking_option': parts[3].strip()
        })
      return policies
  except FileNotFoundError:
    print(f"Error: Policy file not found at {file_path}")
    return None
  except ValueError as e:
    print(f"Error parsing policy file: {e}")
    return None

def generate_policy_name(database, table, column):
  """
  Generates a unique policy name based on database, table and column
  """
  return f"masking-{database}-{table}-{column}"

# --- Main Function ---

def apply_column_masking(policies_to_apply):
    """
    Applies or updates column-level masking policies in Ranger.

    Args:
        policies_to_apply: A list of dictionaries, each containing:
                          - database: The database name.
                          - table: The table name.
                          - column: The column name.
                          - masking_option: The masking option (e.g., 'MASK', 'MASK_SHOW_LAST_4').
    """

    for policy_def in policies_to_apply:
        database = policy_def['database']
        table = policy_def['table']
        column = policy_def['column']
        masking_option = policy_def['masking_option']

        policy_name = generate_policy_name(database, table, column)

        # Check if the policy already exists
        existing_policy = get_ranger_policy(policy_name)

        policy_data = {
            "service": ranger_service_name,
            "name": policy_name,
            "policyType": 0,
            "description": f"Column masking for {database}.{table}.{column}",
            "isAuditEnabled": True,
            "resources": {
                "database": {
                    "values": [database],
                    "isExcludes": False,
                    "isRecursive": False
                },
                "table": {
                    "values": [table],
                    "isExcludes": False,
                    "isRecursive": False
                },
                "column": {
                    "values": [column],
                    "isExcludes": False,
                    "isRecursive": False
                }
            },
            "policyItems": [],  # You might add specific users/groups here if needed
            "denyPolicyItems": [],
            "allowExceptions": [],
            "denyExceptions": [],
            "dataMaskPolicyItems": [
                {
                    "dataMaskInfo": {
                        "dataMaskType": masking_option,
                        "conditionExpr": "",
                        "valueExpr": ""
                    },
                    "accesses": [
                        {
                            "type": "select",
                            "isAllowed": True
                        }
                    ],
                    "users": [],  # Add users who need access if necessary
                    "groups": ["public"],  # Typically, masking is applied to public or a wide group
                    "roles": [],
                    "conditions": []
                }
            ],
            "serviceType": "hive",
            "options": {},
            "validitySchedules": [],
            "policyLabels": [],
            "zoneName": "",
            "isDenyAllElse": True
        }

        if existing_policy:
            # Update the existing policy
            print(f"Updating policy: {policy_name}")
            updated_policy = update_ranger_policy(existing_policy['id'], policy_data)
            if updated_policy:
                print(f"Policy updated successfully: {updated_policy['name']}")
            else:
                print(f"Failed to update policy: {policy_name}")

        else:
            # Create a new policy
            print(f"Creating policy: {policy_name}")
            created_policy = create_ranger_policy(policy_data)
            if created_policy:
                print(f"Policy created successfully: {created_policy['name']}")
            else:
                print(f"Failed to create policy: {policy_name}")

if __name__ == "__main__":
    policies = read_policy_file(policy_file)
    
    if policies is None:
        sys.exit(1) # Exit with an error code

    apply_column_masking(policies)
