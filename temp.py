import smtplib

server_smtp = smtplib.SMTP('smtp.exmail.qq.com')
server_smtp.starttls()

print(server_smtp)
