from exchangelib import DELEGATE, IMPERSONATION, Account, Credentials

credentials = Credentials('maxinzhe@mingshiim.com', 'Ms540436')
my_account = Account(primary_smtp_address='maxinzhe@mingshiim.com', credentials=credentials,
                     autodiscover=True, access_type=DELEGATE)

my_account.ad_response