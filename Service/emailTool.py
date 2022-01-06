import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from email.header import Header
import sys, os
from Local.config_local import admins, smtp_credentials

def sendEmailtoAdmin(subject, body):
    mailServerObject = smtplib.SMTP(smtp_credentials['server'], smtp_credentials['serverPort'])
    mailServerObject.ehlo()
    mailServerObject.starttls()
    mailServerObject.login(smtp_credentials['username'], smtp_credentials['password'])

    msg = MIMEText(body, _charset='UTF-8')
    # msg['From'] = smtp_credentials['from']
    # msg['From'] = smtp_credentials['from']
    # msg['From'] = str(Header('Real Estate <service@carguide.hk>'))
    # msg['From'] = formataddr((str(Header('Real Estate', 'utf-8')), smtp_credentials['from']))
    msg['From'] = formataddr(('Real Estate', smtp_credentials['from']))
    msg['To'] = ", ".join(admins)
    msg['Subject'] = subject

    # from email.utils import formataddr
    # import smtplib
    # from email.message import EmailMessage

    # msg = EmailMessage()
    # msg['From'] = formataddr(('Example Sender Name', 'john@example.com'))
    # msg['To'] = formataddr(('Example Recipient Name', 'jack@example.org'))
    # msg.set_content('Lorem Ipsum')

    mailServerObject.sendmail(smtp_credentials['from'], admins, msg.as_string())

    # messageBody = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\n%s" % (config.smtp_credentials['from'], ", ".join(receiverListObject), "WhatsApp Failure", messageObject)
    # mailServerObject.sendmail(config.smtp_credentials['from'], receiverListObject, messageBody)

    mailServerObject.quit()