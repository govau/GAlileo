from datetime import datetime, timedelta

from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

private_key = Variable.get("git_deploy_private_key_secret")
repo_url = Variable.get("git_remote_url")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 9, 27, 18, 30),
    'retries': 0,
    'email': ['devops@yourorg.com'],
    'email_on_failure': True,
    'catchup': False,
    'depends_on_past': False,
}

name = 'git_sync'

schedule = '* * * * *'

dag = DAG(name, schedule_interval=schedule, default_args=default_args)

git_sync_bash = """
    set -e

    # setup ssh key and command for git
    pwd=$(pwd)
    cat <<EOF > $pwd/id_rsa
{private_key}
EOF
    chmod 0600 $pwd/id_rsa
    cat <<EOF > $pwd/ssh
ssh -i $pwd/id_rsa -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no \\$*
EOF
    chmod +x $pwd/ssh
    export GIT_SSH="$pwd/ssh"

    # clone repo
    repo_url='{repo_url}'
    repo_dir=$(basename $repo_url .git)
    git clone --depth 1 $repo_url

    # gsutil rsync into the gcs bucket dags/git dir
    gsutil -m rsync -x "\\.git\\/.*$" -d -r $repo_dir gs://$GCS_BUCKET/dags/git
""".format(private_key=private_key, repo_url=repo_url)

t1 = BashOperator(
    task_id= 'git_pull',
    bash_command=git_sync_bash,
    dag=dag
)
