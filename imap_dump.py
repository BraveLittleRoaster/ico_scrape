import imaplib
import os

class FetchEmail():

    connection = None
    error = None

    def __init__(self, mail_server, username, password):
        self.connection = imaplib.IMAP4_SSL(mail_server)
        self.connection.login(username, password)
        self.connection.select(readonly=False) # so we can mark mails as read

    def close_connection(self):
        """
        Close the connection to the IMAP server
        """
        self.connection.close()

    def save_attachment(self, msg, download_folder="./tmp"):
        """
        Given a message, save its attachments to the specified
        download folder (default is /tmp)

        return: file path to attachment
        """
        att_path = "No attachment found."
        for part in msg.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue

            filename = part.get_filename()
            att_path = os.path.join(download_folder, filename)

if __name__ == "__main__":

    fm = FetchEmail("export.imap.aol.com:993", "pretish01@aol.com","oomfahmctlvhamwp")