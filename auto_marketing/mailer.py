import smtplib
import os.path
from tqdm import tqdm

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# to attach a file for the email
from email.mime.base import MIMEBase
from email import encoders

class Mailer:
    ''' Class which sends an email through gmail '''

    def __init__(self, email, password):
        self.email, self.password = email, password

    def __setAttachment(self, msg, file_location):
        filename = os.path.basename(file_location)
        attachment = open(file_location, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= {}".format(filename))
        msg.attach(part)
        attachment.close()

    def sendMessage(self, send_to_email, attachment_paths=[], message='', subject=''):
        """ Method which sends a message given an email, if attachment path is provided then we'll
            send this file. """
        msg = MIMEMultipart()
        msg['From'] = self.email
        msg['To'] = send_to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.ehlo()
        # email = 'proyectowebescom@gmail.com'
        # passwd = 'contrasenia123'
        # server.login(email, passwd) # login might fail if credentials don't match, of course
        server.login(self.email, self.password) # login might fail if credentials don't match, of course

        for fpath in tqdm(attachment_paths):
            self.__setAttachment(msg, fpath)
        try:
            server.login(self.email, self.password) # login might fail if credentials don't match, of course
            text = msg.as_string()
            server.sendmail(self.email, send_to_email, text)
            server.quit()
        except smtplib.SMTPAuthenticationError:
            print("Exception: your credential doesn't match")
            return False
        return True
