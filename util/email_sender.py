
from celery import Celery
import smtplib
from email.mime.text import MIMEText

app = Celery('email_sender', broker='pyamqp://username:pass123456@serverurl:port//')


@app.task
def send_email(email, code):
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login('username@gmail.com', 'alsdigneilgsdfse')

    msg = MIMEText(f'pincode: {code}')
    msg['Subject'] = 'Stock Balance Email 인증 코드'
    s.sendmail("verification@stockbalance.com", email, msg.as_string())

    s.quit()
