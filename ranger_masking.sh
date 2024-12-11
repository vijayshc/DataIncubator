#!/bin/bash

# --- Configuration ---
# Read configuration from ranger_config.ini
config_file="ranger_config.ini"

if [ ! -f "$config_file" ]; then
  echo "Error: Configuration file '$config_file' not found."
  exit 1
fi

ranger_host=$(grep "^host" "$config_file" | cut -d '=' -f 2 | tr -d ' ')
ranger_port=$(grep "^port" "$config_file" | cut -d '=' -f 2 | tr -d ' ')
ranger_service_name=$(grep "^service_name" "$config_file" | cut -d '=' -f 2 | tr -d ' ')
ranger_principal=$(grep "^principal" "$config_file" | cut -d '=' -f 2 | tr -d ' ')
ranger_keytab=$(grep "^keytab" "$config_file" | cut -d '=' -f 2 | tr -d ' ')
policy_file=$(grep "^policy_file" "$config_file" | cut -d '=' -f 2 | tr -d ' ')

# --- Ranger API Base URL ---
ranger_base_url="http://${ranger_host}:${ranger_port}/service/public/v2/api"

# --- Functions ---

# Function to get a Ranger policy by name
get_ranger_policy() {
  local policy_name="$1"
  local url="${ranger_base_url}/policy?policyName=${policy_name}"

  # Use curl with Kerberos authentication
  local response=$(curl -s --negotiate -u : -X GET "$url" --keytab "$ranger_keytab" --principal "$ranger_principal" -k)

  if [[ $? -ne 0 ]]; then
      echo "Error getting policy: ${policy_name}"
      echo "$response" 
      return 1
  fi
  
  # Check if the response is not empty and extract the policy
  if [[ -n "$response" ]]; then
      # Check if the response contains 'statusCode' which is usually an error
      if [[ "$response" == *'"statusCode"'* ]]; then
          echo "Error: Received an error response from Ranger:"
          echo "$response"
          return 1
      else
          # If no error, assume it's the policy data
          echo "$response" | jq -r ".[] | select(.name == \"$policy_name\")"
      fi
  else
      # If response is empty, policy does not exist
      echo "Policy not found: ${policy_name}"
      return 1
  fi
}

# Function to create a new Ranger policy
create_ranger_policy() {
  local policy_data="$1"
  local url="${ranger_base_url}/policy"
  local headers="Content-Type: application/json"

  # Use curl with Kerberos authentication
  local response=$(curl -s --negotiate -u : -X POST "$url" -H "$headers" -d "$policy_data" --keytab "$ranger_keytab" --principal "$ranger_principal" -k)

  if [[ $? -ne 0 ]]; then
    echo "Error creating policy."
    echo "$response"
    return 1
  fi

  echo "$response"
}

# Function to update an existing Ranger policy
update_ranger_policy() {
  local policy_id="$1"
  local policy_data="$2"
  local url="${ranger_base_url}/policy/${policy_id}"
  local headers="Content-Type: application/json"

  # Use curl with Kerberos authentication
  local response=$(curl -s --negotiate -u : -X PUT "$url" -H "$headers" -d "$policy_data" --keytab "$ranger_keytab" --principal "$ranger_principal" -k)

  if [[ $? -ne 0 ]]; then
    echo "Error updating policy: ${policy_id}"
    echo "$response"
    return 1
  fi

  echo "$response"
}

# Function to read and parse the policy file
read_policy_file() {
  local file_path="$1"

  if [ ! -f "$file_path" ]; then
    echo "Error: Policy file not found at $file_path"
    return 1
  fi

  # Read the file line by line, skipping comments and empty lines
  while IFS= read -r line; do
    # Skip empty lines and comments
    [[ -z "$line" || "$line" == \#* ]] && continue

    # Check if the line has exactly 4 comma-separated values
    if [[ "$line" != *,*,*,* ]]; then
      echo "Error: Invalid policy format in line: $line"
      continue  # Continue to the next line instead of exiting
    fi

    # Extract database, table, column, and masking_option using IFS
    IFS=',' read -r database table column masking_option <<< "$line"

    # Trim whitespace from extracted values
    database="${database// /}"
    table="${table// /}"
    column="${column// /}"
    masking_option="${masking_option// /}"

    # Output the parsed policy data in a structured format
    echo "database:$database,table:$table,column:$column,masking_option:$masking_option"
  done < "$file_path"

  return 0
}

# Function to generate a unique policy name
generate_policy_name() {
  local database="$1"
  local table="$2"
  local column="$3"
  echo "masking-${database}-${table}-${column}"
}

# --- Main Script ---

# Read and parse the policy file
policies=$(read_policy_file "$policy_file")

# Check if reading the policy file was successful
if [[ $? -ne 0 ]]; then
    echo "Error processing policy file. Exiting."
    exit 1
fi

# Iterate through each policy
while IFS=',' read -r policy_line; do
    # Parse each policy
    IFS=':' read -r _database _table _column _masking_option <<< "$policy_line"
    database="${_database#database}"
    table="${_table#table}"
    column="${_column#column}"
    masking_option="${_masking_option#masking_option}"

    policy_name=$(generate_policy_name "$database" "$table" "$column")

    # Check if the policy already exists
    existing_policy_json=$(get_ranger_policy "$policy_name")
    
    # Extract policy ID using jq, if policy exists
    if [[ -n "$existing_policy_json" ]]; then
        existing_policy_id=$(echo "$existing_policy_json" | jq -r '.id')
    else
        existing_policy_id=""
    fi

  # Construct the policy data using a here-document and jq
  policy_data=$(jq -n \
    --arg service "$ranger_service_name" \
    --arg name "$policy_name" \
    --arg database "$database" \
    --arg table "$table" \
    --arg column "$column" \
    --arg masking_option "$masking_option" \
    '{
      service: $service,
      name: $name,
      policyType: 0,
      description: "Column masking for " + $database + "." + $table + "." + $column,
      isAuditEnabled: true,
      resources: {
        database: { values: [$database], isExcludes: false, isRecursive: false },
        table: { values: [$table], isExcludes: false, isRecursive: false },
        column: { values: [$column], isExcludes: false, isRecursive: false }
      },
      policyItems: [],
      denyPolicyItems: [],
      allowExceptions: [],
      denyExceptions: [],
      dataMaskPolicyItems: [
        {
          dataMaskInfo: { dataMaskType: $masking_option, conditionExpr: null, valueExpr: null },
          accesses: [{ type: "select", isAllowed: true }],
          users: [],
          groups: ["public"],
          roles: [],
          conditions: []
        }
      ],
      serviceType: "hive",
      options: {},
      validitySchedules: [],
      policyLabels: [],
      zoneName: "",
      isDenyAllElse: true
    }')

  if [[ -n "$existing_policy_id" ]]; then
    # Update the existing policy
    echo "Updating policy: $policy_name"
    update_response=$(update_ranger_policy "$existing_policy_id" "$policy_data")
    if [[ $? -eq 0 ]]; then
      echo "Policy updated successfully: $policy_name"
      echo "$update_response" | jq .
    else
      echo "Failed to update policy: $policy_name"
    fi
  else:
    # Create a new policy
    echo "Creating policy: $policy_name"
    create_response=$(create_ranger_policy "$policy_data")
    if [[ $? -eq 0 ]]; then
      echo "Policy created successfully: $policy_name"
      echo "$create_response" | jq .
    else
      echo "Failed to create policy: $policy_name"
    fi
  fi

done <<< "$policies"

echo "Script execution completed."
