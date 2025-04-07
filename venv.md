Okay, let's break down the process of creating a Python virtual environment (venv), packaging it with your custom package, and configuring Oozie (via Hue) to use it for your PySpark application.

This method ensures that your PySpark executors have access to the exact Python environment and packages they need, independent of what's installed cluster-wide.

Core Idea:

Create: Build a self-contained Python virtual environment locally or on an edge node.

Install: Install your custom package and all its dependencies into this venv.

Package: Bundle the entire venv into an archive file (e.g., .tar.gz).

Upload: Place the archive onto HDFS.

Configure Oozie/Spark: Tell Spark (via Oozie configuration) to:

Distribute the archive to each executor node.

Unpack the archive.

Use the Python executable from within the unpacked venv to run your PySpark code.

Detailed Steps:

Step 1: Create the Virtual Environment and Install Packages

Choose a Machine: Do this on your local machine or a cluster edge node where you have Python 3, pip, and internet access (or access to your custom package). Crucially, the Python version used here should ideally match the major.minor version of Python available on your cluster nodes (e.g., if nodes have Python 3.6, create the venv with Python 3.6). Mismatches can sometimes cause subtle issues.

Create venv:

# Choose a directory for your project
mkdir my_pyspark_project
cd my_pyspark_project

# Create the virtual environment (e.g., named 'pyspark_venv')
python3 -m venv pyspark_venv

# Activate the virtual environment
source pyspark_venv/bin/activate

# (Optional but recommended) Upgrade pip and setuptools
pip install --upgrade pip setuptools

# Install your custom package (replace with actual path/name)
# If it's a local directory:
# pip install /path/to/your/custom_package_directory
# If it's installable from a repository or wheel file:
# pip install custom_package_name
# pip install /path/to/your/custom_package.whl
# Make sure to install ALL dependencies your PySpark script needs!
pip install pandas numpy # etc.

# Deactivate the environment (optional for now, needed before packaging)
deactivate


Step 2: Package the Virtual Environment

The best tool for packaging venvs for distribution is venv-pack, as it handles path rewriting correctly.

Install venv-pack (outside the venv is fine):

pip install venv-pack
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END

Package the venv:
Run this command from the directory containing the pyspark_venv directory (i.e., my_pyspark_project in our example).

# This creates pyspark_venv.tar.gz in the current directory
venv-pack -o pyspark_venv.tar.gz -p pyspark_venv/ --force

# Options:
# -o : Output file name
# -p : Path to the venv directory to pack (optional if only one venv in current dir)
# --force : Overwrite output file if it exists
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END

Alternative (less recommended, might have path issues): If you can't use venv-pack, you can try using tar:

# cd into the venv directory first
cd pyspark_venv
# Create the archive from within the venv directory
tar -czf ../pyspark_venv.tar.gz .
cd .. # Go back to the parent directory
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END

Using venv-pack is strongly preferred.

Step 3: Upload the Archive and PySpark Script to HDFS

Upload the venv archive:
Choose a location on HDFS accessible by the user running the Oozie job. Often, this is within your user directory or a dedicated lib folder for your workflow.

# Example: Create a directory for your workflow assets
hdfs dfs -mkdir -p /user/<your_hdfs_user>/oozie_workflows/my_spark_job/lib

# Upload the archive
hdfs dfs -put pyspark_venv.tar.gz /user/<your_hdfs_user>/oozie_workflows/my_spark_job/lib/
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END

Upload your PySpark script:
Your main PySpark application file (your_script.py) also needs to be on HDFS.

# Assuming your_script.py is in the current local directory
hdfs dfs -put your_script.py /user/<your_hdfs_user>/oozie_workflows/my_spark_job/
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END

Step 4: Configure the Oozie Workflow in Hue

Navigate to Hue: Open the Oozie Editor in Hue. Create a new workflow or edit an existing one.

Add a Spark Action: Drag and drop a Spark action onto the workflow graph.

Configure the Spark Action:

Name: Give your action a meaningful name (e.g., spark-custom-env-job).

Jar/Py file: Select the HDFS path to your main PySpark script (/user/<your_hdfs_user>/oozie_workflows/my_spark_job/your_script.py).

Spark opts / Arguments: This is where you might pass command-line arguments to your script, not Spark configuration. Leave blank for now unless needed.

Files: Add the HDFS path to your PySpark script again here if it's not automatically added (sometimes needed depending on Hue version/config). /user/<your_hdfs_user>/oozie_workflows/my_spark_job/your_script.py

Archives: This is crucial. Add the HDFS path to your packaged venv archive with a fragment identifier (#) to create a symlink name on the executor nodes.

Value: /user/<your_hdfs_user>/oozie_workflows/my_spark_job/lib/pyspark_venv.tar.gz#pyspark_venv

The #pyspark_venv part tells YARN to unpack the archive into a directory named pyspark_venv in the executor's working directory.

Configuration Properties (or Spark Options): Add the following properties:

Property 1:

Name: spark.pyspark.python

Value: ./pyspark_venv/bin/python

Explanation: This tells PySpark which Python executable to use on the executor nodes. ./ refers to the current working directory where YARN unpacked the archive into the pyspark_venv directory (because of the #pyspark_venv alias used in the Archives section).

Property 2 (Optional but Recommended for Clarity): Sometimes Hue handles the archive distribution implicitly, but explicitly setting spark.yarn.dist.archives ensures it.

Name: spark.yarn.dist.archives

Value: /user/<your_hdfs_user>/oozie_workflows/my_spark_job/lib/pyspark_venv.tar.gz#pyspark_venv

Explanation: Explicitly tells YARN to distribute and unpack the archive. If you added the archive in the "Archives" field above, Hue might add this automatically, but adding it here guarantees it. If you add it here, you might not strictly need it in the "Archives" field, but it's often clearer to use the dedicated field in Hue. Recommendation: Use the "Archives" field in Hue and verify if spark.yarn.dist.archives gets set correctly in the final Oozie XML, or add it explicitly here for safety.

Other Settings: Configure Master (yarn), Deploy Mode (cluster is typical for Oozie), Executor/Driver Memory, Cores, etc., as needed for your job.

Step 5: Your PySpark Script (your_script.py)

Your Python script itself doesn't need any special changes to activate the environment. Spark handles that based on the configuration you provided. Just import your custom package as usual.

from pyspark.sql import SparkSession
import os

# Import your custom package - this will work because Spark is using the venv Python
import custom_package
# Import other packages installed in the venv
import pandas

# Example: Print python executable path (for debugging)
# import sys
# print("Python Executable:", sys.executable)
# print("Working Directory:", os.getcwd())
# print("Directory Contents:", os.listdir('.')) # See if pyspark_venv is unpacked

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PySpark Custom Env Example") \
        .getOrCreate()

    print("Spark Session Created successfully.")

    # Use your custom package
    result = custom_package.my_function(...)
    print(f"Custom package function result: {result}")

    # Use other packages from venv
    df_pandas = pandas.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    print("Pandas DataFrame created:")
    print(df_pandas.head())

    # Your PySpark logic here...
    # Example: Create a Spark DataFrame
    data = [("Alice", 1), ("Bob", 2)]
    columns = ["name", "id"]
    df_spark = spark.createDataFrame(data, columns)
    df_spark.show()

    print("Job finished successfully.")

    spark.stop()
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Python
IGNORE_WHEN_COPYING_END

Step 6: Save and Run the Oozie Workflow

Save your workflow in Hue.

Submit the workflow.

Monitor the job through the Oozie dashboard in Hue. Check the YARN logs for your Spark application, especially the stderr logs of the executors, if you encounter import errors.

Troubleshooting & Important Notes:

Python Version: Ensure the Python version used to create the venv is compatible with the Python available on the cluster nodes (ideally the same major.minor version).

Permissions: The user running the Oozie workflow must have read access to the venv archive and the PySpark script on HDFS.

venv-pack vs. tar/zip: venv-pack is highly recommended as it correctly handles activating scripts and potential hardcoded paths within the venv, making it truly relocatable. Simple tar or zip might work for simple cases but can fail for complex packages.

Large Environments: Very large venv archives can increase the startup time for your Spark executors as the archive needs to be distributed and unpacked. Keep the venv lean if possible.

Dependencies: Ensure all required packages (your custom one and its dependencies, plus any other libraries like pandas, numpy, etc.) are installed in the venv before packaging.


![image](https://github.com/user-attachments/assets/8d3f09bb-6d73-4057-a0a2-6cc8129f80b1)


YARN Logs: If your job fails with ImportError, check the YARN application logs (accessible via Hue or ResourceManager UI). Look at the stderr logs for the executors. They will show the exact Python error, often revealing which package is missing or if the Python path is incorrect. You can add debug prints like print(sys.executable) or os.listdir('.') in your script to see what the executor sees.

# Alias: The #<alias> part in the archive path is critical. Make sure the alias (pyspark_venv in the example) matches the directory name used in the spark.pyspark.python path (./pyspark_venv/bin/python).
