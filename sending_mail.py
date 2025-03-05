from dotenv import load_dotenv
import os
import smtplib
from email.mime.text import MIMEText
from pymongo import MongoClient
import time


def sendemail_alert(transaction):
    subject = "🚨 Urgent: Suspicious Activity Detected on Your Account! ⚠️"
    body = f"""
    🚨 Immediate Action Required: Unusual Activity on Your Account!

        Dear {transaction['merchant']},

        We have detected suspicious activity on your account and want to ensure your security. 🔍🔒

        Details of the activity:
        📅 Date: {transaction['timestamp']}
        🌍 Location: {transaction['location']}
        💲  Amount:{transaction['amount']}
        💳 Transaction: {transaction['transaction_type']}
        💳 Card Number: {transaction['card_number']}

        If this was not you, please take immediate action:
        ✅ Secure your account by resetting your password.
        ✅ Review recent transactions for any unauthorized activity.
        ✅ Contact our support team at [Customer Support Contact] if you need assistance.
    
    
    """

    msg = MIMEText(body, "plain")
    msg["Subject"] = subject
    msg["From"] = EMAIL_SENDER
    msg["To"] = EMAIL_RECEIVER

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        server.quit()
        print("Mail sent successfully")

    except Exception as e:
        print(e)


def monitor_fraud_transaction():
    last_checked_id = None
    while True:
        latest_fraud = collection.find_one(sort=[("_id", -1)])

        if latest_fraud and latest_fraud["_id"] != last_checked_id:
            print("New fraud detected : ", latest_fraud)
            sendemail_alert(latest_fraud)
            last_checked_id = latest_fraud["_id"]

        time.sleep(1)


if __name__ == "__main__":
    load_dotenv("./.env")
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    EMAIL_SENDER = os.getenv("EMAIL_SENDER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

    clinet = MongoClient(os.getenv("MONGO_URI"))
    df = clinet[os.getenv("MONGO_DB")]
    collection = df[os.getenv("MONGO_COLLECTION")]

    monitor_fraud_transaction()
