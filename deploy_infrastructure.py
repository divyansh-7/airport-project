import os
import snowflake.connector
from dotenv import load_dotenv

# Load hidden credentials from the .env file
load_dotenv()

# Read the raw SQL file
with open('infrastructure/snowflake/02_stage_setup.sql', 'r') as file:
    sql_script = file.read()

# Dynamically inject the secure credentials
sql_script = sql_script.replace('{{AWS_IAM_ROLE_ARN}}', os.getenv('AWS_IAM_ROLE_ARN'))
sql_script = sql_script.replace('{{AWS_S3_BRONZE_URL}}', os.getenv('AWS_S3_BRONZE_URL'))

def create_snowflake_connection():
    """Establishes and returns a secure connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        print("✅ STAGE 1: Securely connected to Snowflake.")
        return conn
    except Exception as e:
        print(f"❌ STAGE 1 ERROR: Connection failed. Check your .env file or network connectivity.\nDetails: {e}")
        return None

def execute_sql_files(conn, sql_dir, files_to_run):
    """Iterates through and executes specified SQL files using an active connection."""
    print("\nInitiating Snowflake Infrastructure Deployment...")
    
    for filename in files_to_run:
        filepath = os.path.join(sql_dir, filename)
        print(f"\nExecuting {filename}...")
        
        # Isolate File Reading
        try:
            with open(filepath, 'r') as file:
                sql_script = file.read()
        except FileNotFoundError:
            print(f"  ❌ STAGE 2 ERROR: File not found at {filepath}. Aborting deployment.")
            return False
        except Exception as e:
            print(f"  ❌ STAGE 2 ERROR: Failed to read {filename}.\nDetails: {e}")
            return False

        # Isolate SQL Execution
        try:
            results = conn.execute_string(sql_script)
            for res in results:
                print(f"  -> {res.fetchall()[0][0]}")
        except snowflake.connector.errors.ProgrammingError as e:
            print(f"  ❌ STAGE 2 ERROR: SQL Compilation/Syntax failure in {filename}.\nDetails: {e}")
            return False
        except Exception as e:
            print(f"  ❌ STAGE 2 ERROR: Execution failed for {filename}.\nDetails: {e}")
            return False
            
    print("\n🚀 SUCCESS: Entire Bronze Architecture deployed to Snowflake!")
    return True

def deploy_snowflake_infrastructure():
    """Orchestrates the connection and execution phases with smart pre-checks."""
    sql_dir = os.path.join("infrastructure", "snowflake")
    
    # Phase 1: Connection
    conn = create_snowflake_connection()
    if not conn:
        print("❌ Deployment aborted due to initialization failure.")
        return

    # Phase 2: Metadata Pre-Check for Storage Integration
    print("\nRunning infrastructure pre-checks...")
    try:
        cursor = conn.cursor()
        # Query Snowflake metadata to see if our integration already exists
        cursor.execute("SHOW STORAGE INTEGRATIONS LIKE 'S3_AIRLINE_INTEGRATION';")
        integration_exists = cursor.fetchone() is not None
        cursor.close()
    except Exception as e:
        print(f"❌ Pre-check failed to read Snowflake metadata: {e}")
        conn.close()
        return

    # Dynamically build the execution list based on state
    files_to_run = ["01_foundation.sql"]
    
    if integration_exists:
        print("  -> 'S3_AIRLINE_INTEGRATION' already exists. Skipping 02_storage_integration.sql to preserve security tokens.")
    else:
        print("  -> 'S3_AIRLINE_INTEGRATION' not found. Adding 02_storage_integration.sql to deployment queue.")
        files_to_run.append("02_storage_integration.sql")
        
    # Append the remaining schema and pipes that are safe to check
    files_to_run.extend(["03_bronze_tables.sql", "04_snowpipe.sql"])

    # Phase 3: Execution
    try:
        # Revert 02_storage_integration.sql back to CREATE OR REPLACE internally 
        # so it runs clean if it ever needs a fresh deploy
        execute_sql_files(conn, sql_dir, files_to_run)
    finally:
        conn.close()
        print("🔒 Snowflake connection securely closed.")

if __name__ == "__main__":
    deploy_snowflake_infrastructure()