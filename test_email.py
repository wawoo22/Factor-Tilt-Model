import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def test_email_setup():
    # Replace with your credentials
    sender_email = "walterwoo6@gmail.com"
    app_password = "spqg vrpi cqcx culn"
    recipient_email = "walterwoo22@gmail.com"
    
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = "Factor System Email Test"
    
    body = "This is a test email from your Factor Monitoring System!"
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, app_password)
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        
        print("✅ Email test successful!")
        return True
        
    except Exception as e:
        print(f"❌ Email test failed: {e}")
        return False

if __name__ == "__main__":
    test_email_setup()
