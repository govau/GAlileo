import airflow
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser
user = PasswordUser(models.User())
user.username = 'new_user_name'
user.email = 'new_user_email@example.com'
user.password = 'set_the_password'
session = settings.Session()
session.add(user)
session.commit()
session.close()
exit()
