import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

def EmailingAttachment(subject, pwd, Sender, recipients, Body, PathToFile ,FileToAttach):


    msg = MIMEMultipart()
    msg['From'] = Sender


    if len(recipients) > 1:
        msg['To'] = ", ".join(recipients)
    else:
        msg['To'] = recipients[0]



    msg['Subject'] = subject
    body = Body
    msg.attach(MIMEText(body, 'plain'))
    filename = FileToAttach
    attachment = open(PathToFile, "rb")



    # instance of MIMEBase and named as p
    p = MIMEBase('application', 'octet-stream')
    # To change the payload into encoded form
    p.set_payload((attachment).read())
    # encode into base64
    encoders.encode_base64(p)
    p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    # attach the instance 'p' to instance 'msg'
    msg.attach(p)
    # creates SMTP session
    s = smtplib.SMTP('smtp.gmail.com', 587)
    # start TLS for security
    s.starttls()
    # Authentication
    s.login(Sender, pwd)
    # Converts the Multipart msg into a string
    text = msg.as_string()
    # sending the mail
    s.sendmail(Sender, recipients, text)
    # terminating the session
    s.quit()
